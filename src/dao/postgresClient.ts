import moment from 'moment'
import {Pool} from 'pg'
import Cursor from 'pg-cursor'

import {ClubTag} from '../model/Tag'
import logger from '../utils/Logger'

const ONLINE_USER_ACTIVITY_LIMIT = 60 * 5

class PostgresClient {
  protected dbpool

  constructor() {
    this.dbpool = new Pool({
      host: process.env.POSTGRES_HOST || '35.228.179.1',
      // host: process.env.POSTGRES_HOST || '10.186.0.7',
      user: process.env.POSTGRES_USER || 'postgres',
      password: process.env.POSTGRES_PASS || 'R36R5NF3iIW8WAU',
      database: process.env.POSTGRES_DB || 'connect_club',
      min: 1,
      max: 10,
      idleTimeoutMillis: 30000,
    })
  }

  // TODO find more elegant method to iterate a cursor via processor
  async streamUsers(processor: (rows: Array<any>) => Promise<void>) {
    const connection = await this.dbpool.connect()
    try {
      const cursor = await connection.query(
        new Cursor(
          `select users.id, users.username, users.name, users.surname, users.recommended_for_following_priority, languages, users.is_tester, 
                  array_agg(distinct followed_users) as followed_users, array_agg(distinct app_contacts) as app_contacts,
                   users.state as state from users 
           left join (
            select user_id as followed_users, follower_id from follow
            inner join (select id from users) u2 on u2.id = follow.user_id
           ) f on f.follower_id = users.id
           left join (
            select pc.owner_id, phone_number, ucc.id as app_contacts from phone_contact pc
            inner join (select phone, id from users) ucc on ucc.phone = pc.phone_number 
           ) contacts on contacts.owner_id = users.id and contacts.phone_number <> users.phone
           where users.username is not NULL
           group by users.id
          `,
        ),
      )
      const readCursor = async () => {
        return new Promise<void>((res, rej) => {
          cursor.read(100, (err: Error, rows: any) => {
            if (err) {
              logger.error(err)
              return rej(err)
            }
            if (!rows || rows.length === 0) {
              cursor.close(() => {
                // connection.release()
                return res()
              })
            } else {
              processor(rows).then(() => {
                return res(readCursor())
              })
            }
          })
        })
      }
      await readCursor()
    } catch (err) {
      logger.error(err)
    } finally {
      connection.release()
    }
  }

  async getClubsWithEvents(): Promise<Array<Record<string, any>>> {
    const connection = await this.dbpool.connect()
    try {
      return (
        await connection.query(
          `
        SELECT c.id, created_at, interest_id as interests, c.is_public, array_agg(evs.cname) as cname
        FROM club c
        LEFT JOIN (select club_id, array_agg(interest_id) as interest_id from club_interest group by club_id) ci on c.id = ci.club_id
        LEFT JOIN 
        (
          SELECT club_id, event_schedule.id, vr.cname from event_schedule
          INNER JOIN (
            SELECT id, event_schedule_id, cm.cname from video_room
            INNER JOIN (SELECT community.name as cname, video_room_id from community) cm on cm.video_room_id = video_room.id
          ) vr on vr.event_schedule_id = event_schedule.id
        ) evs on c.id = evs.club_id        
        GROUP BY c.id, interest_id, is_public
        `,
        )
      ).rows.map((r) => {
        return {
          clubId: r.id,
          createdAt: r.created_at,
          interests: r.interests || null,
          events: r.cname.filter((v: any) => {
            return v !== null
          }),
          isPublic: r.is_public || false,
        }
      })
    } catch (err) {
      logger.error(err)
      return []
    } finally {
      connection.release()
    }
  }

  async streamUserClubs(processor: (rows: Array<any>) => Promise<void>) {
    const connection = await this.dbpool.connect()
    try {
      const cursor = await connection.query(
        new Cursor(
          `
          SELECT
            cp.user_id AS user_id,
            json_agg(json_build_object('clubId', cp.club_id, 'role', cp.role)) AS clubs
          FROM
            club_participant cp
          GROUP BY cp.user_id
          `,
        ),
      )
      const readCursor = async () => {
        return new Promise<void>((res, rej) => {
          cursor.read(100, (err: Error, rows: any) => {
            if (err) {
              logger.error(err)
              return rej(err)
            }
            if (!rows || rows.length === 0) {
              cursor.close(() => {
                return res()
              })
            } else {
              processor(rows).then(() => {
                return res(readCursor())
              })
            }
          })
        })
      }
      await readCursor()
    } catch (err) {
      logger.error(err)
    } finally {
      connection.release()
    }
  }

  async streamUserScheduledEvents(
    processor: (rows: Array<any>) => Promise<void>,
  ) {
    const connection = await this.dbpool.connect()
    try {
      const cursor = await connection.query(
        new Cursor(
          `
          select id, date_time, owner_id, languages, club_id, for_members_only, esp.participants, esi.interests 
          from event_schedule  as es
          left join (select event_id, json_agg(json_build_object('user_id', user_id, 'speaker', is_special_guest)) as participants 
            from event_schedule_participant group by event_id) esp on esp.event_id = es.id
          left join (select event_schedule_id, array_agg(interest_id) as interests 
            from event_schedule_interest group by event_schedule_id) esi on esi.event_schedule_id = es.id
          where date_time > extract(epoch from now())
          `,
        ),
      )
      const readCursor = async () => {
        return new Promise<void>((res, rej) => {
          cursor.read(100, (err: Error, rows: any) => {
            if (err) {
              logger.error(err)
              return rej(err)
            }
            if (!rows || rows.length === 0) {
              cursor.close(() => {
                return res()
              })
            } else {
              processor(rows).then(() => {
                return res(readCursor())
              })
            }
          })
        })
      }
      await readCursor()
    } catch (err) {
      logger.error(err)
    } finally {
      connection.release()
    }
  }

  async streamUsersBio(processor: (rows: Array<any>) => Promise<void>) {
    const connection = await this.dbpool.connect()
    try {
      const cursor = await connection.query(
        new Cursor(
          `select users.id, users.about, short_bio from users 
           where users.username is not NULL and ((users.short_bio is not null and users.short_bio != '') 
            or (users.about is not null and users.about != ''))
          `,
        ),
      )
      const readCursor = async () => {
        return new Promise<void>((res, rej) => {
          cursor.read(100, (err: Error, rows: any) => {
            if (err) {
              logger.error(err)
              return rej(err)
            }
            if (!rows || rows.length === 0) {
              cursor.close(() => {
                // connection.release()
                return res()
              })
            } else {
              processor(rows).then(() => {
                return res(readCursor())
              })
            }
          })
        })
      }
      await readCursor()
    } catch (err) {
      logger.error(err)
    } finally {
      connection.release()
    }
  }

  async getFullUserProfiles(
    uids: Array<number>,
  ): Promise<Array<Record<string, any>>> {
    const connection = await this.dbpool.connect()
    try {
      const res = await connection.query(
        `
            select users.*, photo.processed_name from users 
            left join photo on photo.id = users.avatar_id 
            where users.id in (${uids})
        `,
      )
      return res.rows.map((r) => {
        const userDisabled = (user: any) => {
          // logger.debug(user.deleted, user.bannedAt)
          return user.deleted !== null || user.banned_at !== null
        }
        return {
          id: !userDisabled(r) ? r.id : 0,
          avatar: !userDisabled(r)
            ? r.processed_name
              ? `${process.env.IMAGE_RESIZER_BASE_URL}/:WIDTHx:HEIGHT/${r.processed_name}`
              : null
            : null,
          name: !userDisabled(r)
            ? r.name
            : r.bannedAt !== null
            ? 'Banned'
            : 'Deleted',
          surname: !userDisabled(r) ? r.surname : 'User',
          displayName: !userDisabled(r)
            ? `${r.name} ${r.surname}`
            : 'Banned User',
          about: !userDisabled(r)
            ? r.about
              ? JSON.parse(JSON.stringify(r.about))
              : null
            : '',
          username: !userDisabled(r) ? r.username : 'deleted',
          isDeleted: userDisabled(r),
          createdAt: !userDisabled(r) ? r.createdAt : 0,
          online: !userDisabled(r)
            ? r.onlineInVideoRoom ||
              moment().unix() - r.lastTimeActivity < ONLINE_USER_ACTIVITY_LIMIT
            : false,
          lastSeen: !userDisabled(r) ? r.lastTimeActivity || r.createdAt : 0,
          badges: r.badges || [],
          shortBio: r.shortBio ? JSON.parse(JSON.stringify(r.shortBio)) : null,
          longBio: r.longBio,
        }
      })
    } catch (err) {
      logger.error(err)
      return []
    } finally {
      connection.release()
    }
  }

  async getShortUserProfiles(
    uids: Array<number>,
  ): Promise<Array<Record<string, any>>> {
    const connection = await this.dbpool.connect()
    try {
      const res = await connection.query(
        `
            select users.*, photo.processed_name from users 
            left join photo on photo.id = users.avatar_id 
            where users.id in (${uids})
        `,
      )
      return res.rows.map((r) => {
        const userDisabled = (user: any) => {
          // logger.debug(user.deleted, user.bannedAt)
          return user.deleted !== null || user.banned_at !== null
        }
        return {
          id: !userDisabled(r) ? r.id : 0,
          avatar: r.processed_name,
          displayName: !userDisabled(r)
            ? `${r.name} ${r.surname}`
            : 'Banned User',
          about: !userDisabled(r)
            ? r.about
              ? JSON.parse(JSON.stringify(r.about))
              : null
            : '',
          username: !userDisabled(r) ? r.username : 'deleted',
          isDeleted: userDisabled(r),
          shortBio: r.shortBio ? JSON.parse(JSON.stringify(r.shortBio)) : null,
          longBio: r.longBio,
        }
      })
    } catch (err) {
      logger.error(err)
      return []
    } finally {
      connection.release()
    }
  }

  async getShouldUserAlwaysSeeAllEvents(userId: number): Promise<boolean> {
    const connection = await this.dbpool.connect()
    try {
      const res = await connection.query(
        `
          select always_show_ongoing_upcoming_events from users where id = $1 
        `,
        [userId],
      )
      return res.rows[0].always_show_ongoing_upcoming_events
    } catch (err) {
      logger.error(err)
      return false
    } finally {
      connection.release()
    }
  }

  async getUsersWithWallet(): Promise<Array<Record<string, any>>> {
    const connection = await this.dbpool.connect()
    try {
      const res = await connection.query(
        `
          select id, wallet from users where wallet is not null
        `,
      )
      return res.rows
    } catch (err) {
      logger.error(err)
      return []
    } finally {
      connection.release()
    }
  }

  async getClubById(clubId: string): Promise<ClubTag | null> {
    const connection = await this.dbpool.connect()
    try {
      const res = await connection.query(
        `
          select c.id, c.created_at, ci.interest_id as interests, c.is_public from club c
          left join (select club_id, array_agg(interest_id) as interest_id from club_interest group by club_id) ci on c.id = ci.club_id
          where id = ($1)::uuid
        `,
        [clubId],
      )
      return res.rows.length > 0
        ? {
            clubId: res.rows[0].id,
            createdAt: res.rows[0].created_at,
            interests: res.rows[0].interests,
            isPublic: res.rows[0].is_public,
          }
        : null
    } catch (err) {
      logger.error(err)
      return null
    } finally {
      connection.release()
    }
  }

  async getClubsByIds(
    clubIds: Array<string>,
  ): Promise<Array<Record<string, any>>> {
    const connection = await this.dbpool.connect()
    try {
      const res = await connection.query(
        `
        select club.id, uphoto.processed_name as avatar, club.title, club.description, 
          json_build_object('ownerId', uo.id, 'displayName', uo.name || ' ' || uo.surname, 'avatar', uphoto.processed_name) as owner
        from club
        inner join (select id, name, surname, avatar_id from users) uo on uo.id = club.owner_id
        left join photo as uphoto on uphoto.id = uo.avatar_id 
        left join photo as cphoto on cphoto.id = club.avatar_id 
        where club.id = Any($1::uuid[])
      `,
        [clubIds],
      )
      return res.rows.map((r) => {
        const owner = r.owner
        owner.avatar = `${process.env.IMAGE_RESIZER_BASE_URL}/:WIDTHx:HEIGHT/${owner.avatar}`
        return {
          id: r.id,
          avatar: `${process.env.IMAGE_RESIZER_BASE_URL}/:WIDTHx:HEIGHT/${r.avatar}`,
          title: r.title,
          description: r.description,
          owner: owner,
        }
      })
    } catch (err) {
      logger.error(err)
      return []
    } finally {
      connection.release()
    }
  }

  async getClubForEvent(communityName: string): Promise<string | null> {
    const connection = await this.dbpool.connect()
    try {
      const res = await connection.query(
        `
        select club_id
        from community
        left join (select id, event_schedule_id from video_room) vr on vr.id = community.video_room_id
        left join (select id, club_id from event_schedule) es on es.id = vr.event_schedule_id
        where community.name = $1
      `,
        [communityName],
      )
      return res.rows[0]?.club_id || null
    } catch (err) {
      logger.error(err)
      return null
    } finally {
      connection.release()
    }
  }

  async getEventSchedule(id: string): Promise<Record<string, any> | null> {
    const connection = await this.dbpool.connect()
    try {
      const res = await connection.query(
        `
        select es.*, esp.participants, esi.interests 
        from event_schedule  as es
        left join (select event_id, json_agg(json_build_object('user_id', user_id, 'speaker', is_special_guest)) as participants 
          from event_schedule_participant group by event_id) esp on esp.event_id = es.id
        left join (select event_schedule_id, array_agg(interest_id) as interests 
          from event_schedule_interest group by event_schedule_id) esi on esi.event_schedule_id = es.id
        where id = ($1)::uuid
      `,
        [id],
      )
      return res.rows.length > 0 ? res.rows[0] : null
    } catch (err) {
      logger.error(err)
      return null
    } finally {
      connection.release()
    }
  }

  async getOnlineEventSchedules(): Promise<Array<Record<string, any>>> {
    const connection = await this.dbpool.connect()
    const now = moment.utc().subtract(60, 'minutes').unix()
    try {
      const res = await connection.query(
        `
        select vr.event_schedule_id from video_room as vr
        where vr.started_at >= $1 AND vr.event_schedule_id is not null
      `,
        [now],
      )
      return res.rows
    } catch (err) {
      logger.error(err)
      return []
    } finally {
      connection.release()
    }
  }

  async getUserGeneralInterests(
    userId: number,
  ): Promise<Array<Record<string, any>>> {
    const connection = await this.dbpool.connect()
    try {
      const res = await connection.query(`
        select uint.interest_id, ints.name as interest_name from user_interest as uint 
        inner join (select id, name from interest where is_old = false) ints ON  ints.id = uint.interest_id
        where uint.user_id = ${userId}
      `)
      return res.rows
    } catch (err) {
      logger.error(err)
      return []
    } finally {
      connection.release()
    }
  }

  async getInterests(
    interestIds: number[],
  ): Promise<Array<Record<string, any>>> {
    const connection = await this.dbpool.connect()
    try {
      const res = await connection.query(
        `
        select * from interest
        where id = ANY($1::int[])
      `,
        [interestIds],
      )
      return res.rows
    } catch (err) {
      logger.error(err)
      return []
    } finally {
      connection.release()
    }
  }

  async getUserEventParticipations(
    userId: number,
  ): Promise<Array<Record<string, any>>> {
    const connection = await this.dbpool.connect()
    try {
      const res = await connection.query(`
          SELECT vmp.endpoint_allow_incoming_media AS stage, c.name AS id, esi.interest_id
          FROM video_meeting_participant vmp
          JOIN video_meeting vm on vm.id = vmp.video_meeting_id
          JOIN video_room vr on vr.id = vm.video_room_id
          JOIN community c on vr.id = c.video_room_id
          LEFT JOIN event_schedule_interest esi on esi.event_schedule_id = vr.event_schedule_id
          WHERE vmp.participant_id = ${userId} AND vmp.end_time IS NOT NULL
      `)
      return res.rows
    } catch (err) {
      logger.error(err)
      return []
    } finally {
      connection.release()
    }
  }

  async getUserEventInterests(
    userId: number,
    lastSync: number,
  ): Promise<Array<Record<string, any>>> {
    const connection = await this.dbpool.connect()
    try {
      const res = await connection.query(`
        select esi.interest_id, i.name, count(esi.interest_id) from event_schedule_interest esi
        inner join interest i on i.id = esi.interest_id
        where esi.event_schedule_id in (select esp.event_id from event_schedule_participant esp where esp.user_id = ${userId} and esp.created_at > ${
        lastSync / 1000
      })
        GROUP BY esi.interest_id, i.name
      `)
      return res.rows
    } catch (err) {
      logger.error(err)
      return []
    } finally {
      connection.release()
    }
  }

  async getUserSkills(userId: number): Promise<Array<Record<string, any>>> {
    const connection = await this.dbpool.connect()
    try {
      const res = await connection.query(`
        select uskill.skill_id, skills.name as skill_name, skills.category_id, grp.name as group_name from user_skill as uskill 
        inner join (select id, name, category_id from skill) skills ON skills.id = uskill.skill_id
        inner join skill_category grp on grp.id = skills.category_id 
        where uskill.user_id = ${userId}
      `)
      return res.rows
    } catch (err) {
      logger.error(err)
      return []
    } finally {
      connection.release()
    }
  }

  async getUserIndustries(userId: number): Promise<Array<Record<string, any>>> {
    const connection = await this.dbpool.connect()
    try {
      const res = await connection.query(`
        select uind.industry_id, inds.name as industry_name from user_industry as uind 
        inner join (select id, name from industry) inds ON inds.id = uind.industry_id
        where uind.user_id = ${userId}
      `)
      return res.rows
    } catch (err) {
      logger.error(err)
      return []
    } finally {
      connection.release()
    }
  }

  async getUserGoals(userId: number): Promise<Array<Record<string, any>>> {
    const connection = await this.dbpool.connect()
    try {
      const res = await connection.query(`
        select ugoal.goal_id, goals.name as goal_name from user_goal as ugoal 
        inner join (select id, name from goal) goals ON goals.id = ugoal.goal_id
        where ugoal.user_id = ${userId}
      `)
      return res.rows
    } catch (err) {
      logger.error(err)
      return []
    } finally {
      connection.release()
    }
  }

  async getUserEventActivities(
    userId: number,
    lastSync: number,
  ): Promise<Array<Record<string, any>>> {
    const connection = await this.dbpool.connect()
    try {
      const res = await connection.query(`
        select cm.name as id, esi.interest_id  from video_room_event vre
        inner join community cm on cm.video_room_id = vre.video_room_id
        left join (select id, event_schedule_id from video_room) vr on vr.id = vre.video_room_id
        left join event_schedule_interest esi on esi.event_schedule_id = vr.event_schedule_id
        where vre.user_id = ${userId} and vre.time > ${lastSync} and vre."event" = 'moveToStage'
      `)
      return res.rows
    } catch (err) {
      logger.error(err)
      return []
    } finally {
      connection.release()
    }
  }

  async getVerifiedUsers(): Promise<Array<number>> {
    const connection = await this.dbpool.connect()
    try {
      const res = await connection.query(
        `
            select u.id from users u where  u."state" = 'verified' and (u.is_tester = FALSE OR u.is_tester = null)
        `,
      )
      return res.rows.map((r) => {
        return Number.parseInt(r.id)
      })
    } catch (e) {
      logger.error(e)
      return []
    }
  }
}

const postgresClient = new PostgresClient()
export default postgresClient
