import * as _ from 'lodash'
import {isEmpty} from 'lodash'
import moment from 'moment'
import neo4j, {Driver, Session} from 'neo4j-driver'

import {PGUserBase} from '../model/PGUser'
import {
  BehaviorTag,
  ClubJoinTag,
  ClubRoleChange,
  ClubTag,
  ExtendedTag,
  TagsGroupedByLayer,
  TagType,
} from '../model/Tag'
import {UserListWithTotal, UserToken, UserWithMatches} from '../model/User'
import {sleep} from '../utils/common'
import logger from '../utils/Logger'

// const uri = 'neo4j+s://981ad2af.databases.neo4j.io'
// const uri = 'neo4j+s://3f50d83a.databases.neo4j.io:7687' // Igor
const uri = process.env.NEO4J_URL || 'neo4j://10.196.15.238'

// const uri = process.env.NEO4J_URL || 'neo4j://10.186.15.201'

class Neo4jClient {
  protected driver: Driver

  constructor() {
    this.driver = neo4j.driver(
      uri,
      // neo4j.auth.basic('neo4j', 'UBwHRo8lBWUW7DFBYPusr1r_geFc5hl3fppciDHs3tw'),
      // neo4j.auth.basic('neo4j', '7r0HSDYJz9USLIrprjnNYTZslj5xzJetYgAKfPiwnIQ'), // Igor
      neo4j.auth.basic(
        process.env.NEO4J_USER || 'neo4j',
        process.env.NEO4J_PASS || 'OWQxOTVkZTcyYWRl',
      ),
      {
        maxConnectionPoolSize: 50,
      },
    )
  }

  async bootstrap(): Promise<boolean> {
    const session: Session = this.driver.session()
    await this.initScheme(session)
    return await this.checkNoUsers(session)
  }

  async getUserMatches(userId: number): Promise<UserWithMatches | undefined> {
    const session: Session = this.driver.session({
      defaultAccessMode: neo4j.session.READ,
    })
    try {
      const uResult = await session.run(
        `
              MATCH (u:User {userId: $userId})
              OPTIONAL MATCH (u)-[ht:HAS_TAG]->(t)-[:BELONGS_TO_LAYER]->(l:Layer)
              WITH u, collect({tag: t, layer: l.name}) as tags
              OPTIONAL MATCH (u)-[hm]->(:Meeting)
              WITH u, tags, collect(hm) as meets
              RETURN u as user, tags, meets     
          `,
        {userId},
      )
      const mResult = await session.run(
        `
              MATCH (me:User {userId: $userId})-[ht]->(mtag)<-[ht2]-(muser:User)
              WHERE  
                id(me) <> id(muser)
                AND labels(mtag) in [['Tag'], ['Meeting']]
                AND NOT (me)-[:FOLLOWS]->(muser) 
                AND NOT (me)-[:HAS_PHONE_CONTACT]->(muser)
                AND (muser.isTester is null OR muser.isTester = false)
              WITH me, muser, 
                collect({tag: mtag, score: case mtag.score when null then ht2.score else mtag.score end}) as mtags
              RETURN muser, mtags, reduce(score = 0, n IN mtags | score + n.score) AS score
              ORDER BY score desc   
              LIMIT 200      
          `,
        {userId},
      )
      const uRec = uResult.records[0].get('user').properties
      const tRec = uResult.records[0].get('tags')
      const mRec = uResult.records[0].get('meets')
      const tRecGrouped = _.groupBy(tRec, (tr: Record<string, any>) => {
        return tr.layer
      })
      let tags: Array<TagType> = []
      for (const ll of Object.keys(tRecGrouped)) {
        tags = tags.concat(
          tRecGrouped[ll]
            .filter((t) => {
              return t.tag !== null
            })
            .map((v) => {
              const props = v.tag.properties
              props.layer = ll
              return props
            }),
        )
      }
      const meets = {
        participated: mRec.filter((m: Record<string, any>) => {
          return m.type === 'PARTICIPATED_IN_EVENT'
        }).length,
        spoke: mRec.filter((m: Record<string, any>) => {
          return m.type === 'SPOKE_AT_EVENT'
        }).length,
      }
      // logger.debug('tags', tags)
      const matches = mResult.records.map((dRec) => {
        const matchedTags = dRec
          .get('mtags')
          .map((mt: Record<string, Record<string, any>>) => {
            const props = mt.tag.properties
            const layer = tRec.find((tr: Record<string, any>) => {
              return (
                tr.tag !== null &&
                tr.tag.properties.id.toString() === props.id.toString() &&
                tr.tag.properties.category === props.category
              )
            })
            // logger.debug('mt.score', mt.score, neo4j.isInt(mt.score))
            return {
              id: props.id.toString(),
              category: props.category,
              score: neo4j.isInt(mt.score) ? mt.score.toNumber() : mt.score,
              categoryName: props.categoryName,
              interestName: props.interestName,
              layer: layer?.layer || 'behavior_layer',
            }
          })
        const score: number = neo4j.isInt(dRec.get('score'))
          ? dRec.get('score').toNumber()
          : dRec.get('score')
        return {
          userId: dRec.get('muser').properties.userId.toNumber(),
          avatar: null,
          about: null,
          username: dRec.get('muser').properties.username,
          fullName: dRec.get('muser').properties.fullName,
          score: score,
          matchedTags: matchedTags.filter((m: Record<string, any>) => {
            return m.category !== null && m.category !== undefined
          }),
          meetsNumber: matchedTags.filter((m: Record<string, any>) => {
            return m.category === null || m.category === undefined
          }).length,
        }
      })
      const parseTag = (tag: Record<string, any>): TagType => {
        let rTag: Record<string, any> = {
          score: tag.score.toNumber(),
          id: tag.id.toString(),
          category: tag.category,
          layer: tag.layer,
        }
        if (tag.interestName) {
          rTag = rTag as ExtendedTag
          rTag.interestName = tag.interestName
          rTag.categoryName = tag.categoryName
        }
        return rTag as TagType
      }
      return {
        userId: uRec.userId.toNumber(),
        username: uRec.username,
        fullName: uRec.fullName,
        skills: tags
          .filter((tag: Record<string, any>) => tag.category === 'skills')
          .map((tag: Record<string, any>) => parseTag(tag)),
        industry: tags
          .filter((tag: Record<string, any>) => tag.category === 'industry')
          .map((tag: Record<string, any>) => parseTag(tag)),
        professionalGoals: tags
          .filter((tag: Record<string, any>) => tag.category === 'goals')
          .map((tag: Record<string, any>) => parseTag(tag)),
        socialGoals: tags
          .filter((tag: Record<string, any>) => tag.category === 'social_goals')
          .map((tag: Record<string, any>) => parseTag(tag)),
        lookingFor: tags
          .filter(
            (tag: Record<string, any>) => tag.category === 'looking_for_def',
          )
          .map((tag: Record<string, any>) => parseTag(tag)),
        generalInterests: tags
          .filter((tag: Record<string, any>) => tag.layer === 'general_layer')
          .map((tag: Record<string, any>) => parseTag(tag) as ExtendedTag),
        behavior: meets,
        matches: matches,
      }
    } catch (error) {
      logger.error(error)
    } finally {
      await session.close()
    }
  }

  /*
   * method used by mobile app for the user recommendations tab
   */
  async getUserRecommendations(
    userId: number,
    limit: number,
    offset: number,
    closeSession = true,
  ): Promise<Array<number> | null> {
    const session: Session = this.driver.session({
      defaultAccessMode: neo4j.session.READ,
    })
    try {
      // PAY ATTENTION TO DOUBLED `WITH` CLAUSES
      // This is because of restrictions described in https://neo4j.com/developer/kb/post-union-processing/
      const mResult = await session.run(
        `
              MATCH (me:User {userId: $userId})
              CALL {
                  WITH me
                  WITH me as user
                  MATCH (me)-[:HAS_PHONE_CONTACT]->(muser)
                  WHERE NOT (me)-[:FOLLOWS]->(muser) 
                    AND muser.state = 'verified' 
                    AND size(apoc.coll.intersection(me.languages, muser.languages)) > 0
                  RETURN muser, 0 as priority, 0 as score
                  UNION ALL
                  WITH me
                  WITH me
                  MATCH (me)-[:HAS_NFT]-()-[]->(s:SmartContract)<-[]-()<-[:HAS_NFT]-(muser:User)
                  WHERE s.contractType = 'ERC721' 
                    AND me.userId <> muser.userId
                    AND muser.state = 'verified' 
                    AND size(apoc.coll.intersection(me.languages, muser.languages)) > 0
                    AND (muser.isTester is null OR muser.isTester = false)
                    AND NOT (me)-[:HAS_PHONE_CONTACT]->(muser)
                    AND NOT (me)-[:FOLLOWS]->(muser) 
                  WITH distinct(s) as s, me, muser
                  WITH me, muser, collect({tag: s, score: 50}) as mtags 
                  RETURN muser, 1 as priority, reduce(score = 0, n IN mtags | score + n.score) AS score
                  UNION ALL
                  WITH me
                  WITH me
                  MATCH (me)-[ht]->(mtag)<-[ht2]-(muser:User)
                  WHERE  
                    id(me) <> id(muser)
                    AND labels(mtag) in [['Tag'], ['Meeting'], ['Club'], ['NftToken']]
                    AND muser.state = 'verified' 
                    AND size(apoc.coll.intersection(me.languages, muser.languages)) > 0
                    AND (muser.isTester is null OR muser.isTester = false)
                    AND NOT (me)-[:HAS_PHONE_CONTACT]->(muser)
                    AND NOT (me)-[:FOLLOWS]->(muser) 
                  WITH me, muser, collect({tag: mtag, score: 
                      case labels(mtag) in [['NftToken']] when true then 
                          case mtag.totalCount < 100 when true then 50 else 10 end
                        else
                        case mtag.score when null then ht2.score else mtag.score end 
                      end
                    }) as mtags
                  RETURN muser, 2 as priority, reduce(score = 0, n IN mtags | score + n.score) AS score
              }
              WITH *
              RETURN distinct(muser.userId) as userId, priority, score
              ORDER BY priority asc, score desc, userId desc
              SKIP TOINTEGER($offset)
              LIMIT TOINTEGER($limit)        
          `,
        {userId, limit, offset},
      )
      return mResult.records.map((dRec) => {
        return dRec.get('userId').toNumber()
      })
    } catch (error) {
      logger.error(error)
      return null
    } finally {
      if (closeSession) {
        await session.close()
      }
    }
  }

  async getClubsRecommendations(
    userId: number,
    limit: number,
    offset: number,
    closeSession = true,
  ): Promise<Array<string> | null> {
    const session: Session = this.driver.session({
      defaultAccessMode: neo4j.session.READ,
    })
    try {
      const result = await session.run(
        `
              MATCH (me:User {userId: $userId})
              OPTIONAL MATCH (me)-[:HAS_TAG]->(t:Tag {category: 'gen_interest'})
              WITH me, collect(TOINTEGER(t.id)) as interests
              CALL {
                  WITH me, interests
                  WITH me, interests
                  MATCH (me)-[:HAS_PHONE_CONTACT|:FOLLOWS]->(:User)-[cref]->(clubs:Club {isPublic: true})
                  WHERE NOT (me)-[]->(clubs)
                  WITH distinct(clubs) as clubs, cref
                  CALL {
                    WITH clubs
                    OPTIONAL MATCH (clubs)-[ref:CLUB_HOST_MEETING]->()
                    RETURN count(ref) as eventsCount
                  }
                  RETURN  clubs, 0 as priority, sum(cref.score) as score, eventsCount
                  UNION
                  WITH me, interests
                  WITH me, interests
                  MATCH (clubs:Club {isPublic: true})
                  WHERE size(apoc.coll.intersection(interests, clubs.interests)) > 0
                      AND NOT (clubs)<-[]-(:User)<-[:FOLLOWS|:HAS_PHONE_CONTACT]-(me)
                      AND NOT (me)-[]->(clubs)
                  CALL {
                    WITH clubs
                    OPTIONAL MATCH (clubs)-[ref:CLUB_HOST_MEETING]->()
                    RETURN count(ref) as eventsCount
                  }
                  RETURN clubs, 1 as priority, 1 * size(apoc.coll.intersection(interests, clubs.interests)) as score, eventsCount
              }
              WITH *
              RETURN clubs.id as clubId, priority, score, eventsCount
              ORDER BY priority asc, score desc, clubs.createdAt desc, eventsCount desc
              SKIP TOINTEGER($offset)
              LIMIT TOINTEGER($limit)                      
      `,
        {userId, limit, offset},
      )
      return result.records.map((dRec) => {
        return dRec.get('clubId')
      })
    } catch (error) {
      logger.error(error)
      return null
    } finally {
      if (closeSession) {
        await session.close()
      }
    }
  }

  async getUpcomingEventsRecommendations(
    userId: number,
    limit: number,
    offset: number,
    isSuperUser: boolean,
    openEvents: Array<string>,
    closeSession = true,
  ): Promise<Array<string> | null> {
    const session: Session = this.driver.session({
      defaultAccessMode: neo4j.session.READ,
    })
    try {
      const result = !isSuperUser
        ? await session.run(
            `
              MATCH (me:User {userId: $userId})
              OPTIONAL MATCH (me)-[:HAS_TAG]->(t:Tag {category: 'gen_interest'})
              WITH me, collect(TOINTEGER(t.id)) as interests
              CALL {
                  WITH me, interests
                  WITH me, interests
                  MATCH (me)-[]->(m:MeetingSchedule)
                  WHERE m.startTime > timestamp() / 1000 - 60*60
                  RETURN m
                  UNION 
                  WITH me, interests
                  WITH me, interests
                  MATCH (me)-[:FOLLOWS]->(:User)-[:SCHEDULED_MEETING|:ADMIN_SCHEDULED_AT_MEETING]->(m:MeetingSchedule)
                  WHERE m.membersOnly = false AND (m.isPrivate = false OR m.isPrivate is null)
                  WITH distinct(m) as m, me.languages as langs
                  WHERE m.startTime > timestamp() / 1000 - 60*60 AND size(apoc.coll.intersection(langs, m.languages)) > 0
                  RETURN m
                  UNION
                  WITH me, interests
                  WITH me, interests
                  MATCH (me)-[]->(:Club)-[]->(m:MeetingSchedule)
                  WITH distinct(m) as m, me.languages as langs
                  WHERE m.startTime > timestamp() / 1000 - 60*60 
                    AND size(apoc.coll.intersection(langs, m.languages)) > 0
                  RETURN m
                  UNION 
                  WITH me, interests
                  WITH me, interests
                  MATCH (me)-[:FOLLOWS]->(:User)-[:SPEAKER_SCHEDULED_AT_MEETING]->(m:MeetingSchedule)
                  WHERE m.membersOnly = false AND (m.isPrivate = false OR m.isPrivate is null)
                  WITH distinct(m) as m, me.languages as langs
                  WHERE m.startTime > timestamp() / 1000 - 60*60 AND size(apoc.coll.intersection(langs, m.languages)) > 0
                  RETURN m
                  UNION 
                  WITH me, interests
                  WITH me, interests
                  MATCH (m:MeetingSchedule)
                  WHERE size(apoc.coll.intersection(interests, m.interests)) > 0
                    AND m.startTime > timestamp() / 1000 - 60*60 
                    AND m.membersOnly = false
                    AND (m.isPrivate = false OR m.isPrivate is null)
                    AND size(apoc.coll.intersection(me.languages, m.languages)) > 0
                  RETURN m
              }
              WITH *
              WHERE NOT m.id in $openEvents
              RETURN m.id as mid
              ORDER BY m.startTime asc
              SKIP TOINTEGER($offset)
              LIMIT TOINTEGER($limit)                      
      `,
            {userId, limit, offset, openEvents},
          )
        : await session.run(
            `
              MATCH (m:MeetingSchedule)
              WHERE m.startTime > timestamp() / 1000 - 60*60 AND (m.isPrivate = false OR m.isPrivate is null)
              RETURN m.id as mid
              ORDER BY m.startTime asc
              SKIP TOINTEGER($offset)
              LIMIT TOINTEGER($limit)                      
      `,
            {userId, limit, offset},
          )
      return result.records.map((dRec) => {
        return dRec.get('mid')
      })
    } catch (error) {
      logger.error(error)
      return null
    } finally {
      if (closeSession) {
        await session.close()
      }
    }
  }

  async getCalendarEventsRecommendations(
    userId: number,
    limit: number,
    offset: number,
    isSuperUser: boolean,
    openEvents: Array<string>,
    closeSession = true,
  ): Promise<Array<string> | null> {
    const session: Session = this.driver.session({
      defaultAccessMode: neo4j.session.READ,
    })
    try {
      const result = !isSuperUser
        ? await session.run(
            `
              MATCH (me:User {userId: $userId})
              OPTIONAL MATCH (me)-[:HAS_TAG]->(t:Tag {category: 'gen_interest'})
              WITH me, collect(TOINTEGER(t.id)) as interests
              CALL {
                  WITH me, interests
                  WITH me, interests
                  MATCH (me)-[]->(m:MeetingSchedule)
                  WHERE m.startTime > timestamp() / 1000 - 60*60
                  RETURN m, 0.1 * m.startTime as priority
                  UNION 
                  WITH me, interests
                  WITH me, interests
                  MATCH (me)-[:FOLLOWS]->(:User)-[:SCHEDULED_MEETING|:ADMIN_SCHEDULED_AT_MEETING]->(m:MeetingSchedule)
                  WHERE m.membersOnly = false AND (m.isPrivate = false OR m.isPrivate is null)
                  WITH distinct(m) as m, me.languages as langs
                  WHERE m.startTime > timestamp() / 1000 - 60*60 AND size(apoc.coll.intersection(langs, m.languages)) > 0
                  RETURN m, 0.2 * m.startTime as priority
                  UNION
                  WITH me, interests
                  WITH me, interests
                  MATCH (me)-[]->(:Club)-[]->(m:MeetingSchedule)
                  WITH distinct(m) as m, me.languages as langs
                  WHERE m.startTime > timestamp() / 1000 - 60*60 
                    AND size(apoc.coll.intersection(langs, m.languages)) > 0
                  RETURN m, 0.3 * m.startTime as priority
                  UNION 
                  WITH me, interests
                  WITH me, interests
                  MATCH (me)-[:FOLLOWS]->(:User)-[:SPEAKER_SCHEDULED_AT_MEETING]->(m:MeetingSchedule)
                  WHERE m.membersOnly = false AND (m.isPrivate = false OR m.isPrivate is null)
                  WITH distinct(m) as m, me.languages as langs
                  WHERE m.startTime > timestamp() / 1000 - 60*60 AND size(apoc.coll.intersection(langs, m.languages)) > 0
                  RETURN m, 0.4 * m.startTime as priority
                  UNION 
                  WITH me, interests
                  WITH me, interests
                  MATCH (m:MeetingSchedule)
                  WHERE size(apoc.coll.intersection(interests, m.interests)) > 0
                    AND m.membersOnly = false
                    AND (m.isPrivate = false OR m.isPrivate is null)
                    AND m.startTime > timestamp() / 1000 - 60*60
                    AND size(apoc.coll.intersection(me.languages, m.languages)) > 0
                  RETURN m, 0.5 * m.startTime as priority
              }
              WITH *
              WHERE NOT m.id in $openEvents
              WITH *
              ORDER BY m.startTime, priority
              WITH distinct(m)
              RETURN m.id as mid
              SKIP TOINTEGER($offset)
              LIMIT TOINTEGER($limit)                      
      `,
            {userId, limit, offset, openEvents},
          )
        : await session.run(
            `
              MATCH (m:MeetingSchedule)
              WHERE m.startTime > timestamp() / 1000 - 60*60 AND (m.isPrivate = false OR m.isPrivate is null)
              RETURN m.id as mid
              ORDER BY m.startTime asc
              SKIP TOINTEGER($offset)
              LIMIT TOINTEGER($limit)                      
      `,
            {userId, limit, offset},
          )
      return result.records.map((dRec) => {
        return dRec.get('mid')
      })
    } catch (error) {
      logger.error(error)
      return null
    } finally {
      if (closeSession) {
        await session.close()
      }
    }
  }

  async getUserTokens(userId: number): Promise<Array<UserToken> | null> {
    const session: Session = this.driver.session({
      defaultAccessMode: neo4j.session.READ,
    })
    try {
      const result = await session.run(
        `
              MATCH (u:User { userId: $userId })-[:HAS_NFT]->(t)
              CALL {
                  WITH t
                  RETURN apoc.convert.fromJsonMap(t.metadata) as metadata
              }
              WITH *
              WHERE metadata.image IS NOT NULL
              RETURN metadata.name as name, t.tokenId as tokenId, metadata.image as image, metadata.description as description                 
      `,
        {userId},
      )
      return result.records.map((r) => ({
        name: r.get('name')?.toString() || '',
        tokenId: r.get('tokenId')?.toString() || '',
        image: r.get('image').toString(),
        description: r.get('description')?.toString() || '',
      }))
    } catch (error) {
      logger.error(error)
      return null
    } finally {
      await session.close()
    }
  }

  async getTokens(
    limit: number,
    offset: number,
  ): Promise<Array<UserToken> | null> {
    const session: Session = this.driver.session({
      defaultAccessMode: neo4j.session.READ,
    })
    try {
      const result = await session.run(
        `
              MATCH (u:User)-[:HAS_NFT]->(t)
              CALL {
                  WITH t
                  RETURN apoc.convert.fromJsonMap(t.metadata) as metadata
              }
              WITH *
              WHERE metadata.image IS NOT NULL
              RETURN u.userId as userId, metadata.name as name, t.tokenId as tokenId, metadata.image as image, metadata.description as description          
              SKIP $offset LIMIT $limit      
      `,
        {
          limit: neo4j.int(limit),
          offset: neo4j.int(offset),
        },
      )
      return result.records.map((r) => ({
        userId: r.get('userId').toNumber() || '',
        name: r.get('name')?.toString() || '',
        tokenId: r.get('tokenId')?.toString() || '',
        image: r.get('image').toString(),
        description: r.get('description')?.toString() || '',
      }))
    } catch (error) {
      logger.error(error)
      return null
    } finally {
      await session.close()
    }
  }

  async getTotalTokens(): Promise<number | null> {
    const session: Session = this.driver.session({
      defaultAccessMode: neo4j.session.READ,
    })
    try {
      const result = await session.run(
        `
              MATCH (:User)-[:HAS_NFT]->(t)
              CALL {
                  WITH t
                  RETURN apoc.convert.fromJsonMap(t.metadata) as metadata
              }
              WITH *
              WHERE metadata.image IS NOT NULL
              RETURN count(t) as tc                
      `,
      )

      return result.records[0].get('tc').toNumber()
    } catch (error) {
      logger.error(error)
      return null
    } finally {
      await session.close()
    }
  }

  async getUsers(
    limit: number,
    offset: number,
    orderBy: string,
    orderDir: string,
    searchTerm: string,
  ): Promise<UserListWithTotal> {
    const session: Session = this.driver.session({
      defaultAccessMode: neo4j.session.READ,
    })
    try {
      const total = isEmpty(searchTerm)
        ? await session.run(
            `MATCH (u: User)
               RETURN count(u) as uc`,
            {},
          )
        : await session.run(
            `MATCH (u: User)
               WHERE apoc.text.indexOf(toLower(u.fullName), $searchTerm) > -1 
               RETURN count(u) as uc`,
            {searchTerm: searchTerm.toLowerCase()},
          )
      const term = isEmpty(searchTerm) ? null : searchTerm
      const result =
        orderBy === 'tc'
          ? await session.run(
              `
            MATCH (u:User)-[:HAS_TAG|:SPOKE_AT_EVENT|:PARTICIPATED_IN_EVENT]->(t)
            WHERE u.state = 'verified' AND 
              case $searchTerm when null then 1=1 else apoc.text.indexOf(toLower(u.fullName), $searchTerm) > -1 end 
            RETURN u, count(t) as tc
            ORDER BY tc ${orderDir}
            SKIP $offset LIMIT $limit
        `,
              {
                searchTerm: term,
                orderBy: orderBy,
                limit: neo4j.int(limit),
                offset: neo4j.int(offset),
              },
            )
          : await session.run(
              `
            MATCH (u:User)-[:HAS_TAG|:SPOKE_AT_EVENT|:PARTICIPATED_IN_EVENT]->(t)
            WHERE u.state = 'verified' AND 
              case $searchTerm when null then 1=1 else apoc.text.indexOf(toLower(u.fullName), $searchTerm) > -1 end
              AND case when $orderBy = 'u.totalMatches' then u.totalMatches is not null else 1=1 end 
            RETURN u, count(t) as tc
            ORDER BY case 
              when $orderBy = 'u.totalMatches' then ${orderBy}  
              else tolower(${orderBy}) end ${orderDir}
            SKIP $offset LIMIT $limit
        `,
              {
                searchTerm: term,
                orderBy: orderBy,
                limit: neo4j.int(limit),
                offset: neo4j.int(offset),
              },
            )
      const records = result.records.map((r) => {
        const record = r.get('u').properties
        return {
          userId: record.userId.toNumber(),
          username: record.username as string,
          fullName: record.fullName as string,
          numOfTag: r.get('tc').toNumber(),
          matches: record.totalMatches.toNumber(),
        }
      })
      return {
        userRecords: records,
        total: total.records[0].get('uc').toNumber(),
      }
    } catch (error) {
      logger.error(error)
      return {userRecords: [], total: 0}
    } finally {
      await session.close()
    }
  }

  async getUserIdsWithLastSync(): Promise<Array<Record<string, any>>> {
    const session: Session = this.driver.session({
      defaultAccessMode: neo4j.session.READ,
    })
    try {
      const result = await session.run(
        `
        MATCH (u: User)
        RETURN u.userId, u.lastEventParticipationSync, u.lastEventActivitiesSync
        `,
        {},
      )
      return result.records.map((r) => {
        return {
          userId: r.get('u.userId').toNumber(),
          lastEventParticipationSync: r
            .get('u.lastEventParticipationSync')
            .toNumber(),
          lastEventActivitiesSync: r
            .get('u.lastEventActivitiesSync')
            .toNumber(),
        }
      })
    } catch (error) {
      logger.error(error)
      return []
    } finally {
      await session.close()
    }
  }

  async mergeUsers(records: Array<PGUserBase>, retries = 0): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      for (const record of records) {
        if (record.recommended_for_following_priority != null) {
          record.recommended_for_following_priority = neo4j.int(
            record.recommended_for_following_priority,
          )
        }
        await txc
          .run(
            `MERGE (user: User {userId: TOINTEGER($record.userId)}) 
              ON CREATE SET user.recommended_for_following_priority = $record.recommended_for_following_priority, user.username = $record.username, user.fullName = $record.fullName, user.languages = $record.languages, user.lastEventParticipationSync = TOINTEGER(0), user.lastEventActivitiesSync = TOINTEGER(0), user.state = $record.state, user.isTester = $record.isTester 
              ON MATCH SET user.fullName = COALESCE($record.fullName, user.fullName), user.username = $record.username, user.languages = COALESCE($record.languages, user.languages), user.recommended_for_following_priority = COALESCE($record.recommended_for_following_priority, user.recommended_for_following_priority), user.state = COALESCE($record.state, user.state), user.isTester = $record.isTester
              WITH user, 
                   [(user)-[:FOLLOWS]->(f) WHERE f.userId IN $followedUsers | f] AS foundFollows,
                   [(user)-[:HAS_PHONE_CONTACT]->(f) WHERE f.userId IN $pcUsers | f] AS foundPhoneContacts
              CALL {
                WITH user, foundFollows, foundPhoneContacts
                
                UNWIND [x IN $followedUsers WHERE NOT x IN [ff IN foundFollows | ff.userId] | x] AS newFollow
                MERGE (u2: User {userId: TOINTEGER(newFollow)})
                  ON CREATE SET u2.lastEventParticipationSync = TOINTEGER(0), u2.lastEventActivitiesSync = TOINTEGER(0)
                MERGE (user)-[:FOLLOWS]->(u2)
                WITH user, foundPhoneContacts
                UNWIND [x IN $pcUsers WHERE NOT x IN [ff IN foundPhoneContacts | ff.userId] | x] AS newPC
                MERGE (u3: User {userId: TOINTEGER(newPC)})
                  ON CREATE SET u3.lastEventParticipationSync = TOINTEGER(0), u3.lastEventActivitiesSync = TOINTEGER(0)
                MERGE (user)-[:HAS_PHONE_CONTACT]->(u3)                
                
                RETURN 1
              }
              RETURN user`,
            {
              record: record,
              followedUsers: record.followedUsers
                .filter((fu) => {
                  return fu !== null
                })
                .map((fu) => {
                  return neo4j.int(fu!)
                }),
              pcUsers: record.phoneContactsUsers
                .filter((fu) => {
                  return fu !== null
                })
                .map((fu) => {
                  return neo4j.int(fu!)
                }),
            },
          )
          .catch((err) => {
            throw err
          })
      }
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(500)
        return this.mergeUsers(records, retries + 1)
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async mergeUserTags(
    userId: number,
    newTags: Array<ExtendedTag>,
    layerName: 'professional_layer' | 'goals_layer' | 'general_layer' | string,
    replaceTags: {replaceByCategory: string} | boolean = false,
    retries = 0,
  ): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      const tags = newTags.map((v, idx) => {
        return `MERGE (t${idx}:Tag {category: '${
          v.category
        }', id: '${v.id.toString()}'})
                  ON CREATE SET t${idx}.score = TOINTEGER(${
          v.score
        }), t${idx}.interestName = "${
          v.interestName
        }", t${idx}.categoryName = "${v.categoryName}"
                MERGE (t${idx})-[:BELONGS_TO_LAYER]->(pl) 
                MERGE (u)-[:HAS_TAG]->(t${idx})\n`
      })

      let replaceTagsQuery = ''
      if (typeof replaceTags === 'boolean' && replaceTags) {
        replaceTagsQuery = `OPTIONAL MATCH (u)-[ht:HAS_TAG]->(:Tag)-[:BELONGS_TO_LAYER]->(pl)\n`
      } else if (typeof replaceTags === 'object' && replaceTags !== null) {
        replaceTagsQuery = `OPTIONAL MATCH (u)-[ht:HAS_TAG]->(:Tag {category: '${replaceTags.replaceByCategory}'})-[:BELONGS_TO_LAYER]->(pl)\n`
      }
      if (replaceTagsQuery !== '') {
        replaceTagsQuery += `DELETE ht\n` + (tags.length ? 'WITH u, pl\n' : '')
      }

      await txc
        .run(
          'MATCH (u: User {userId: TOINTEGER($userId)})\n' +
            'MATCH (pl: Layer {name: $layerName})\n' +
            (replaceTagsQuery !== ''
              ? replaceTagsQuery
              : !tags.length
              ? 'RETURN u\n'
              : '') +
            tags.join('\n'),
          {userId, layerName},
        )
        .catch((err) => {
          throw err
        })
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(500)
        return this.mergeUserTags(
          userId,
          newTags,
          layerName,
          replaceTags,
          retries + 1,
        )
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async addUserFollows(
    ownerId: number,
    userId: number,
    retries = 0,
  ): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      await txc
        .run(
          `
            MATCH (o: User {userId: TOINTEGER($ownerId)})
            MATCH (u: User {userId: TOINTEGER($userId)})
            MERGE (o)-[:FOLLOWS {followedOn: $followedOn}]->(u)`,
          {ownerId, userId, followedOn: moment().unix()},
        )
        .catch((err) => {
          throw err
        })
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(500)
        return this.addUserFollows(ownerId, userId, retries + 1)
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async removeUserFollows(
    ownerId: number,
    userId: number,
    retries = 0,
  ): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      await txc
        .run(
          `
            MATCH (:User {userId: TOINTEGER($ownerId)})-[f:FOLLOWS]->(:User {userId: TOINTEGER($userId)})
            DELETE f
            `,
          {ownerId, userId},
        )
        .catch((err) => {
          throw err
        })
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(500)
        return this.removeUserFollows(ownerId, userId, retries + 1)
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async mergeUserBehaviorActivity(
    records: Array<{userId: number; activities: Array<BehaviorTag>}>,
    retries = 0,
  ): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      await txc
        .run(
          `
          MATCH (pl: Layer {name: "behavior_layer"})
          UNWIND $records as row
          MATCH (u: User {userId: TOINTEGER(row.userId)}) 
          UNWIND  row.activities as activity
          MERGE (t:Meeting {id: toString(activity.id)})
            ON CREATE SET t.interests = activity.interests
          MERGE (t)-[:BELONGS_TO_LAYER]->(pl)
          WITH t, u, activity
          CALL apoc.do.when(activity.category = 'userMeetingParticipate',
                  'MERGE (u)-[ht:PARTICIPATED_IN_EVENT {score: activity.score}]->(t) RETURN ht',
                  'MERGE (u)-[ht:SPOKE_AT_EVENT {score: activity.score}]->(t) RETURN ht',
                  {activity: activity, u: u, t: t})
          YIELD value
          RETURN true
          `,
          {records},
        )
        .catch((err) => {
          throw err
        })
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(500)
        return this.mergeUserBehaviorActivity(records, retries + 1)
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async mergeUserNfts(
    userId: number,
    records: Array<Record<string, any>>,
    retries = 0,
  ): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      await txc.run(
        `
          MATCH (pl: Layer {name: "general_layer"})
          MATCH (u: User {userId: TOINTEGER($userId)}) 
          UNWIND $records as row
          MERGE (s:SmartContract {tokenAddress: row.token_address})
            ON CREATE SET s.contractType = row.contract_type
          MERGE (t:NftToken {tokenId: row.token_address + '_' + row.token_id})
            ON CREATE SET t.name = row.name, t.symbol = row.symbol, t.metadata = row.metadata, 
              t.totalCount = TOINTEGER(row.totalCount)
            ON MATCH SET t.totalCount = COALESCE(TOINTEGER(row.totalCount), t.totalCount)
          MERGE (t)-[:BELONGS_TO_SMART_CONTRACT]->(s)   
          MERGE (t)-[:BELONGS_TO_LAYER]->(pl)
          MERGE (u)-[:HAS_NFT]->(t)
          RETURN true  
        `,
        {records, userId},
      )
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(500)
        return this.mergeUserNfts(userId, records, retries + 1)
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async mergeUserWallet(
    userId: number,
    wallet: string | null,
    lastSync: number,
    retries = 0,
  ): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      await txc.run(
        `
          MATCH (u: User {userId: TOINTEGER($userId)})
          SET u.wallet = $wallet, u.walletLastSync = TOINTEGER($lastSync)
          RETURN true  
        `,
        {wallet, userId, lastSync},
      )
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(500)
        return this.mergeUserWallet(userId, wallet, lastSync, retries + 1)
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async removeUserWallet(userId: number, retries = 0): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      await txc.run(
        `
          MATCH (u: User {userId: TOINTEGER($userId)})
          OPTIONAL MATCH (u)-[ref]->(:NftToken)         
          SET u.wallet = null
          DELETE ref
          RETURN true  
        `,
        {userId},
      )
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(500)
        return this.removeUserWallet(userId, retries + 1)
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async mergeUserClubs(
    records: Array<{
      userId: number
      clubs: Array<ClubJoinTag>
    }>,
    retries = 0,
  ): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      await txc
        .run(
          `
          MATCH (pl: Layer {name: "behavior_layer"})
          UNWIND $records as row
          MATCH (u: User {userId: TOINTEGER(row.userId)}) 
          UNWIND row.clubs as club
          MERGE (t:Club {id: toString(club.clubId)})
            ON CREATE SET t.interests = toIntegerList(club.interests), t.createdAt = TOINTEGER(club.createdAt), 
              t.isPublic = club.isPublic 
            ON MATCH SET t.interests = COALESCE(toIntegerList(club.interests), t.interests), t.isPublic = COALESCE(club.isPublic, t.isPublic)
          MERGE (t)-[:BELONGS_TO_LAYER]->(pl)
          WITH t, u, club
          CALL apoc.do.case([
              club.role = 'owner',
              'MERGE (u)-[ht:OWNER_AT_CLUB {score: 4}]->(t) RETURN ht',
              club.role = 'moderator',
              'MERGE (u)-[ht:ADMIN_AT_CLUB {score: 3}]->(t) RETURN ht'
            ],
            'MERGE (u)-[ht:MEMBER_AT_CLUB {score: 1}]->(t) RETURN ht',
            {club: club, u: u, t: t})
          YIELD value
          RETURN true
          `,
          {records},
        )
        .catch((err) => {
          throw err
        })
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(500)
        return this.mergeUserClubs(records, retries + 1)
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async userLeaveClub(
    userId: number,
    clubId: string,
    retries = 0,
  ): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      await txc
        .run(
          `
          MATCH (u: User {userId: TOINTEGER($userId)})-[ref]->(c:Club {id: toString($clubId)}) 
          DELETE ref
          RETURN true
          `,
          {userId, clubId},
        )
        .catch((err) => {
          throw err
        })
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(500)
        return this.userLeaveClub(userId, clubId, retries + 1)
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async mergeUserEventSchedule(
    data: Array<Record<string, any>>,
    retries = 0,
  ): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      await txc
        .run(
          `
          MATCH (pl: Layer {name: "behavior_layer"})
          UNWIND $data as schedule
          MATCH (u:User {userId: TOINTEGER(schedule.owner_id)})
          MERGE (s:MeetingSchedule {id: toString(schedule.id)})
            ON CREATE SET s.startTime = TOINTEGER(schedule.date_time), s.languages = schedule.languages,
              s.membersOnly = schedule.for_members_only, s.interests = toIntegerList(schedule.interests),
              s.isPrivate = schedule.is_private
            ON MATCH SET s.languages = COALESCE(schedule.languages, s.languages), 
              s.interests = COALESCE(toIntegerList(schedule.interests), s.interests), 
              s.startTime = COALESCE(TOINTEGER(schedule.date_time), s.startTime),
              s.isPrivate = COALESCE(schedule.is_private, s.isPrivate)   
          MERGE (s)-[:BELONGS_TO_LAYER]->(pl)
          MERGE (u)-[:SCHEDULED_MEETING]->(s)
          WITH s, schedule
          CALL apoc.do.when(
            schedule.club_id is not null,
            'MATCH (c:Club {id: schedule.club_id}) MERGE (c)-[:CLUB_SCHEDULED_MEETING]->(s)',
            'RETURN true',
            {s: s, schedule: schedule}
          )
          YIELD value
          WITH s, schedule
          CALL {
            WITH s, schedule
            UNWIND schedule.participants as pt
            WITH s, schedule, pt 
            CALL apoc.do.case([
              TOINTEGER(pt.user_id) <> TOINTEGER(schedule.owner_id) AND pt.speaker = true,
              'MATCH (us: User {userId: TOINTEGER(pt.user_id)}) MERGE (us)-[:SPEAKER_SCHEDULED_AT_MEETING]->(s) RETURN true',
              TOINTEGER(pt.user_id) <> TOINTEGER(schedule.owner_id) AND pt.speaker = false,
              'MATCH (us: User {userId: TOINTEGER(pt.user_id)}) MERGE (us)-[:ADMIN_SCHEDULED_AT_MEETING]->(s) RETURN true'
              ],
              'RETURN true',
              {s: s, schedule: schedule, pt: pt}
            )            
            YIELD value
            RETURN true
          }
          RETURN true
        `,
          {data},
        )
        .catch((err) => {
          throw err
        })
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(500)
        return this.mergeUserEventSchedule(data, retries + 1)
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async updateEventSchedule(
    data: Record<string, any>,
    retries = 0,
  ): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      await txc
        .run(
          `
          MATCH (s:MeetingSchedule {id: toString($data.id)})
          SET s.languages = COALESCE($data.languages, s.languages), 
              s.interests = COALESCE(toIntegerList($data.interests), s.interests), 
              s.startTime = COALESCE(TOINTEGER($data.date_time), s.startTime)   
          RETURN true
        `,
          {data},
        )
        .catch((err) => {
          throw err
        })
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(500)
        return this.updateEventSchedule(data, retries + 1)
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async removeEventSchedule(
    data: Record<string, any>,
    retries = 0,
  ): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      await txc
        .run(
          `
          MATCH (s:MeetingSchedule {id: toString($data.id)})
          MATCH (:User)-[ref]->(s)
          OPTIONAL MATCH (:Club)-[ref2]->(s)
          MATCH (s)-[ref3]->(:Layer)
          DELETE ref, ref2, ref3
          DELETE s
          RETURN true
        `,
          {data},
        )
        .catch((err) => {
          throw err
        })
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(500)
        return this.updateEventSchedule(data, retries + 1)
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async mergeClubsWithEvents(
    data: Array<Record<string, any>>,
    retries = 0,
  ): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      await txc
        .run(
          `
          MATCH (pl: Layer {name: "behavior_layer"})
          UNWIND $data as club
          MERGE (t:Club {id: toString(club.clubId)})
            ON CREATE SET t.interests = toIntegerList(club.interests), t.createdAt = TOINTEGER(club.createdAt), 
              t.isPublic = club.isPublic 
            ON MATCH SET t.interests = COALESCE(toIntegerList(club.interests), t.interests), t.isPublic = COALESCE(club.isPublic, t.isPublic)
          MERGE (t)-[:BELONGS_TO_LAYER]->(pl)
          WITH t, club, pl
          CALL {
            WITH t, club, pl
            UNWIND club.events as ev
            MERGE (m:Meeting {id: toString(ev)})
              ON CREATE SET t.interests = club.interests
            MERGE (m)-[:BELONGS_TO_LAYER]->(pl)
            MERGE (t)-[:CLUB_HOST_MEETING]->(m)
            RETURN true
          }
          RETURN true
        `,
          {data},
        )
        .catch((err) => {
          throw err
        })
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(500)
        return this.mergeClubsWithEvents(data, retries + 1)
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async userClubRoleChange(
    data: ClubRoleChange,
    retries = 0,
  ): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      await txc
        .run(
          `
          MATCH (u: User {userId: TOINTEGER($data.userId)}) 
          MATCH (c: Club {id: $data.clubId})
          MATCH (u)-[ref]->(c) 
          DELETE ref
          WITH u, c, $data as data
          CALL apoc.do.case([
              data.role = 'owner',
              'MERGE (u)-[ht:OWNER_AT_CLUB {score: 4}]->(c) RETURN ht',
              data.role = 'moderator',
              'MERGE (u)-[ht:ADMIN_AT_CLUB {score: 3}]->(c) RETURN ht'
            ],
            'MERGE (u)-[ht:MEMBER_AT_CLUB {score: 1}]->(c) RETURN ht',
            {data: data, u: u, c: c})
          YIELD value
          RETURN true
        `,
          {data},
        )
        .catch((err) => {
          throw err
        })
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(500)
        return this.userClubRoleChange(data, retries + 1)
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async clubInterestsChange(
    data: {clubId: string; interests: Array<string>},
    retries = 0,
  ): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      await txc
        .run(
          `
          MATCH (c: Club {id: $data.clubId})
          SET c.interests = toIntegerList($data.interests)
          RETURN true
        `,
          {data},
        )
        .catch((err) => {
          throw err
        })
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(500)
        return this.clubInterestsChange(data, retries + 1)
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async clubPrivacyChange(
    data: {clubId: string; isPublic: boolean},
    retries = 0,
  ): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      await txc
        .run(
          `
          MATCH (c: Club {id: $data.clubId})
          SET c.isPublic = $data.isPublic
          RETURN true
        `,
          {data},
        )
        .catch((err) => {
          throw err
        })
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(500)
        return this.clubPrivacyChange(data, retries + 1)
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async updateUserBehaviorLastSync(
    userId: number,
    lastSync: number,
    syncType: string,
  ): Promise<void> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      await txc
        .run(
          `
          MERGE (u: User {userId: TOINTEGER($userId)})
          ON MATCH SET u.${syncType} = TOINTEGER($lastSync)
        `,
          {userId, lastSync},
        )
        .catch((err) => {
          throw err
        })
      await txc.commit()
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
    } finally {
      await session.close()
    }
  }

  async getSkillsTags(): Promise<Array<ExtendedTag>> {
    const session: Session = this.driver.session({
      defaultAccessMode: neo4j.session.READ,
    })
    try {
      const result = await session.run(
        `
        MATCH (t:Tag {category: 'skills'})-[r:BELONGS_TO_LAYER]->(l:Layer {name: 'professional_layer'}) 
        RETURN t as tags
        `,
        {},
      )
      return result.records.map((rec) => {
        const r = rec.get('tags')
        return {
          id: r.properties.id,
          category: 'skills',
          categoryName: r.properties.categoryName,
          interestName: r.properties.interestName,
          layer: 'professional_layer',
          score: r.properties.score.toNumber(),
        }
      })
    } catch (err: any) {
      logger.error(err)
      return []
    } finally {
      await session.close()
    }
  }

  async getTagsGroupedByLayer(): Promise<TagsGroupedByLayer> {
    const session: Session = this.driver.session()
    try {
      const result = await session.run(
        `
        MATCH p=(t:Tag)-[r:BELONGS_TO_LAYER]->(l:Layer) 
        RETURN l.name, collect(t) AS tags
        `,
        {},
      )
      return result.records.map((r) => ({
        layer: r.get('l.name'),
        tags: r.get('tags').map((t: Record<string, any>) => {
          return Object.assign({}, t.properties, {
            id: t.properties.id.toString(),
            nodeId: t.identity.toNumber(),
            category: t.properties.category.toString(),
            score: t.properties.score.toNumber(),
          })
        }),
      }))
    } catch (error) {
      logger.error(error)
      return []
    } finally {
      await session.close()
    }
  }

  async updateDeviceContactsRelation(
    ownerId: number,
    userIds: number[],
    retries = 0,
  ): Promise<boolean> {
    const session: Session = this.driver.session()
    const txc = session.beginTransaction()
    try {
      await txc
        .run(
          userIds.length
            ? `
          MATCH (u: User {userId: TOINTEGER($ownerId)})
          UNWIND $userIds as userId
          MATCH (u2: User {userId: TOINTEGER(userId)})
          OPTIONAL MATCH (u)-[hpc:HAS_PHONE_CONTACT]->(u2:User)
          CALL apoc.do.when(hpc is null, 'MERGE (u)-[:HAS_PHONE_CONTACT]->(u2) RETURN true', 'RETURN false', {hpc: hpc, u: u, u2: u2})
          YIELD value
          RETURN value
        `
            : `
          MATCH (:User {userId: TOINTEGER($ownerId)})-[hpc:HAS_PHONE_CONTACT]->(:User)
          DELETE hpc
        `,
          {ownerId, userIds},
        )
        .catch((err) => {
          throw err
        })
      await txc.commit()
      return true
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      if (retries < 5) {
        await sleep(100 * (retries + 1))
        return this.updateDeviceContactsRelation(ownerId, userIds, retries + 1)
      } else {
        return false
      }
    } finally {
      await session.close()
    }
  }

  async updateTagsScore(scores: Record<string, any>) {
    if (Object.keys(scores).length > 0) {
      const session: Session = this.driver.session()
      const txc = session.beginTransaction()
      try {
        for (const layer in scores) {
          if (scores[layer].all !== '') {
            await txc
              .run(
                `
                      MATCH (t:Tag)-[r:BELONGS_TO_LAYER]->(l:Layer { name: $layer })
                      SET t.score=TOINTEGER($score)
            `,
                {layer: layer, score: scores[layer].all},
              )
              .catch((err) => {
                throw err
              })
          } else {
            const ids = []
            for (const nodeId in scores[layer]) {
              if (nodeId !== 'all') {
                ids.push({
                  id: nodeId.substring(1),
                  score: scores[layer][nodeId],
                })
              }
            }
            if (ids.length) {
              await txc
                .run(
                  `
                        UNWIND $ids AS tag
                        MATCH (t:Tag)-[r:BELONGS_TO_LAYER]->(l:Layer { name: $layer })
                        WHERE ID(t)=TOINTEGER(tag.id)
                        SET t.score=TOINTEGER(tag.score)
              `,
                  {ids, layer},
                )
                .catch((err) => {
                  throw err
                })
            }
          }
        }
        await txc.commit()
      } catch (error) {
        try {
          await txc.rollback()
        } catch (e) {
          logger.error('##############################')
          logger.error('error in rollback', e)
        }
        logger.error(error)
        return false
      } finally {
        await session.close()
      }
    }
    return true
  }

  async getVerifiedUsers(): Promise<Array<number>> {
    const session: Session = this.driver.session()
    const res = await session.run(`
      MATCH (u:User) WHERE u.state = 'verified' AND (u.isTester is null OR u.isTester = false) 
      RETURN u.userId
    `)
    return res.records.map((r) => {
      return r.get('u.userId').toNumber()
    })
  }

  async getUsersHavingWallet(
    syncTime: number,
    limit: number,
  ): Promise<Array<Record<string, any>>> {
    const session: Session = this.driver.session({
      defaultAccessMode: neo4j.session.READ,
    })
    try {
      const res = await session.run(
        `
      MATCH (u:User) 
      WHERE u.state = 'verified' AND u.wallet is not null 
        AND TOINTEGER($syncTime) - u.walletLastSync > 60*60*24 
      RETURN u.userId, u.wallet
      LIMIT TOINTEGER($limit)
    `,
        {syncTime, limit},
      )
      return res.records.map((r) => {
        return {userId: r.get('u.userId').toNumber(), wallet: r.get('u.wallet')}
      })
    } finally {
      await session.close()
    }
  }

  async getMatchingStats(): Promise<Record<string, any>> {
    const session: Session = this.driver.session({
      defaultAccessMode: neo4j.session.READ,
    })
    const txc = session.beginTransaction()
    try {
      const totalUsers = await txc
        .run(
          `MATCH (u:User) WHERE u.state = 'verified' AND (u.isTester is null OR u.isTester = false) 
                  RETURN count(u) as c`,
          {},
        )
        .catch((err) => {
          throw err
        })
      const totalUsersWOTags = await txc
        .run(
          `MATCH (u:User)
                 WHERE u.state = 'verified' AND (u.isTester is null OR u.isTester = false) 
                  AND NOT (u)-[:HAS_TAG]->() AND NOT (u)-[:PARTICIPATED_IN_EVENT]->() AND NOT (u)-[:SPOKE_AT_EVENT]->()
                 RETURN count(u) as c`,
          {},
        )
        .catch((err) => {
          throw err
        })
      const totalUsersWOTagsBuWithContacts = await txc
        .run(
          `MATCH (u:User)
                  WHERE u.state = 'verified' 
                  AND (u.isTester is null OR u.isTester = false) 
                  AND NOT (u)-[:HAS_TAG]->() 
                  AND NOT (u)-[:PARTICIPATED_IN_EVENT]->() 
                  AND NOT (u)-[:SPOKE_AT_EVENT]->() 
                  AND (u)-[:HAS_PHONE_CONTACT]->(:User)
                  RETURN count(u) as c`,
          {},
        )
        .catch((err) => {
          throw err
        })
      const usersWithAbove10MatchesByProfLayer = await txc
        .run(
          `MATCH (t:Tag)-[:BELONGS_TO_LAYER]->(:Layer {name: 'professional_layer'})
                  MATCH (u:User)-[:HAS_TAG]->(t)<-[:HAS_TAG]-(u2:User)
                  WHERE u.userId <> u2.userId 
                  WITH u, count(u2) as n
                  WHERE n > 10
                  RETURN count(u) as c`,
          {},
        )
        .catch((err) => {
          throw err
        })
      const usersWithAtLeast1MatchByProfLayer = await txc
        .run(
          `MATCH (t:Tag)-[:BELONGS_TO_LAYER]->(:Layer {name: 'professional_layer'})
                  MATCH (u:User)-[:HAS_TAG]->(t)<-[:HAS_TAG]-(u2:User)
                  WHERE u.userId <> u2.userId 
                  WITH u, count(u2) as n
                  WHERE n >= 1
                  RETURN count(u) as c`,
          {},
        )
        .catch((err) => {
          throw err
        })
      const usersWithAbove10MatchesByGoals = await txc
        .run(
          `MATCH (t:Tag)-[:BELONGS_TO_LAYER]->(:Layer {name: 'goals_layer'})
                  MATCH (u:User)-[:HAS_TAG]->(t)<-[:HAS_TAG]-(u2:User)
                  WHERE u.userId <> u2.userId 
                  WITH u, count(u2) as n
                  WHERE n > 10
                  RETURN count(u) as c`,
          {},
        )
        .catch((err) => {
          throw err
        })
      const usersWithAtLeast1MatchByGoals = await txc
        .run(
          `MATCH (t:Tag)-[:BELONGS_TO_LAYER]->(:Layer {name: 'goals_layer'})
                  MATCH (u:User)-[:HAS_TAG]->(t)<-[:HAS_TAG]-(u2:User)
                  WHERE u.userId <> u2.userId 
                  WITH u, count(u2) as n
                  WHERE n >= 1
                  RETURN count(u) as c`,
          {},
        )
        .catch((err) => {
          throw err
        })
      const usersHavingMatchesByMeets = await txc
        .run(
          `MATCH (u:User)-[rel]->(:Meeting)<-[rel2]-(u2:User)
                  WHERE u.userId <> u2.userId AND u.state = 'verified' AND u2.state = 'verified'
                      AND NOT (u)-[:FOLLOWS]->(u2) AND NOT (u)-[:HAS_PHONE_CONTACT]->(u2)
                  WITH u, count(distinct(u2.userId)) as n
                  WHERE n >= 1 
                  RETURN count(distinct(u)) as c`,
          {},
        )
        .catch((err) => {
          throw err
        })
      const usersHavingAbove10MatchesByInterests = await txc
        .run(
          `MATCH (t:Tag)-[:BELONGS_TO_LAYER]->(:Layer {name: 'general_layer'})
                  MATCH (u:User)-[:HAS_TAG]->(t)<-[:HAS_TAG]-(u2:User)
                  WHERE u.userId <> u2.userId 
                  WITH u, count(u2) as n
                  WHERE n > 10
                  RETURN count(u) as c`,
          {},
        )
        .catch((err) => {
          throw err
        })
      await txc.commit()
      return {
        totalUsers: totalUsers.records[0].get('c').toNumber(),
        totalUsersWOTags: totalUsersWOTags.records[0].get('c').toNumber(),
        totalUsersWOTagsBuWithContacts:
          totalUsersWOTagsBuWithContacts.records[0].get('c').toNumber(),
        usersWithAbove10MatchesByProfLayer:
          usersWithAbove10MatchesByProfLayer.records[0].get('c').toNumber(),
        usersWithAtLeast1MatchByProfLayer:
          usersWithAtLeast1MatchByProfLayer.records[0].get('c').toNumber(),
        usersWithAbove10MatchesByGoals:
          usersWithAbove10MatchesByGoals.records[0].get('c').toNumber(),
        usersWithAtLeast1MatchByGoals: usersWithAtLeast1MatchByGoals.records[0]
          .get('c')
          .toNumber(),
        usersHavingMatchesByMeets: usersHavingMatchesByMeets.records[0]
          .get('c')
          .toNumber(),
        usersHavingAbove10MatchesByInterests:
          usersHavingAbove10MatchesByInterests.records[0].get('c').toNumber(),
      }
    } catch (error) {
      try {
        await txc.rollback()
      } catch (e) {
        logger.error('##############################')
        logger.error('error in rollback', e)
      }
      logger.error(error)
      return {}
    } finally {
      await session.close()
    }
  }

  protected async initScheme(session: Session) {
    try {
      await session.run(
        // 'MATCH (n) DETACH DELETE n ' +
        'MERGE (pl: Layer {name: "professional_layer"})\n' +
          'MERGE (gl: Layer {name: "goals_layer"})\n' +
          'MERGE (ll: Layer {name: "looking_for_layer"})\n' +
          'MERGE (bl: Layer {name: "behavior_layer"})\n' +
          'MERGE (grl: Layer {name: "general_layer"})\n',
        {},
      )
      await session.run(
        'CREATE CONSTRAINT user_userId IF NOT EXISTS ON (user:User) ASSERT user.userId IS UNIQUE',
        {},
      )
      await session.run(
        'CREATE INDEX user_isTester IF NOT EXISTS FOR (u:User) ON (u.isTester)',
        {},
      )
      await session.run(
        'CREATE INDEX user_state IF NOT EXISTS FOR (u:User) ON (u.state)',
        {},
      )
      await session.run(
        'CREATE CONSTRAINT meeting_id IF NOT EXISTS ON (m:Meeting) ASSERT m.id is UNIQUE',
        {},
      )
      await session.run(
        'CREATE INDEX tag_id_category IF NOT EXISTS FOR (t:Tag) ON (t.id, t.category)',
        {},
      )
      await session.run(
        'CREATE INDEX user_recommended IF NOT EXISTS FOR (u:User) ON (u.recommended_for_following_priority)',
        {},
      )
      await session.run(
        'CREATE INDEX club_created_at IF NOT EXISTS FOR (c:Club) ON (c.createdAt)',
        {},
      )
      await session.run(
        'CREATE INDEX club_interests IF NOT EXISTS FOR (c:Club) ON (c.interests)',
        {},
      )
      await session.run(
        'CREATE INDEX meeting_schedule_private IF NOT EXISTS FOR (m:MeetingSchedule) ON (m.membersOnly)',
        {},
      )
      await session.run(
        'CREATE INDEX meeting_schedule_start IF NOT EXISTS FOR (m:MeetingSchedule) ON (m.startTime)',
        {},
      )
      await session.run(
        'CREATE INDEX nft_token_address IF NOT EXISTS FOR (t:NftToken) ON (t.tokenAddress)',
        {},
      )
      await session.run(
        'CREATE INDEX nft_token_contract IF NOT EXISTS FOR (t:NftToken) ON (t.contractType)',
        {},
      )
    } catch (error: any) {
      logger.error(error)
    }
  }

  protected async checkNoUsers(session: Session) {
    try {
      const res = await session.run('MATCH (u:User) RETURN u LIMIT 1')
      return res.records.length === 0
    } catch (error: any) {
      logger.error(error)
      return true
    } finally {
      await session.close()
    }
  }
}

const neo4jClient = new Neo4jClient()
export default neo4jClient
