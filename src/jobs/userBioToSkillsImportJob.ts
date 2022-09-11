import neo4jClient from '../dao/neo4jClient'
import postgresClient from '../dao/postgresClient'
import {sleep} from '../utils/common'
import logger from '../utils/Logger'

export class UserBioToSkillsImportJob {
  async doImport(): Promise<void> {
    const skills = (await neo4jClient.getSkillsTags())
      .map((s) => {
        return s.interestName
      })
      .filter((s) => {
        return (
          s.toLowerCase() !== 'go' &&
          s.toLowerCase() !== 'ios' &&
          s.toLowerCase() !== 'social media' &&
          s.toLowerCase() !== 'android'
        )
      })
    logger.debug('starting import users with bio from PG')
    await postgresClient.streamUsersBio(async (rows: Array<any>) => {
      logger.debug('got new rows from PG', rows.length)
      const doMerge = async () => {
        await sleep(100)
        for (const r of rows) {
          const text: string = r.about || r.short_bio
          let reg = skills.join(')\\b|\\b(').toLowerCase()
          reg = '\\b('.concat(reg).concat(')\\b')
          reg = reg.replace(/[.*+?^${}]/g, '\\$&').replace(/ /gi, '\\s')
          // logger.debug(reg)
          const regexp = new RegExp(reg, 'gm')
          let matches = regexp.exec(text.toLowerCase())
          while (matches !== null) {
            logger.info(matches[0])
            matches = regexp.exec(text)
          }
        }
        return true
        // return await neo4jClient.mergeUsers(
        //   rows.map((rec: Record<string, any>) => {
        //     return {
        //       userId: rec.id,
        //       username: rec.username,
        //       fullName: `${rec.name} ${rec.surname}`,
        //       followedUsers: rec.followed_users,
        //       phoneContactsUsers: rec.app_contacts,
        //       languages: rec.languages,
        //       recommended_for_following_priority:
        //         rec.recommended_for_following_priority,
        //       state: rec.state,
        //       isTester: rec.is_tester,
        //     }
        //   }),
        // )
      }
      let retries = 0
      while (!(await doMerge())) {
        if (retries === 4) {
          logger.error('NEO4J users merge failed, reties exhausted!')
          break
        }
        retries++
        logger.warn('NEO4J users merge failed, retrying', retries)
        await sleep(1000 * retries)
        await doMerge()
      }
    })
  }
}
