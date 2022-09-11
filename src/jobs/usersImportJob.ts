import neo4jClient from '../dao/neo4jClient'
import postgresClient from '../dao/postgresClient'
import {sleep} from '../utils/common'
import logger from '../utils/Logger'

export class UsersImportJob {
  async doImport(): Promise<void> {
    // do something
    logger.debug('starting import users from PG')
    await postgresClient.streamUsers(async (rows: Array<any>) => {
      logger.debug('got new rows from PG', rows.length)
      const doMerge = async () => {
        return await neo4jClient.mergeUsers(
          rows.map((rec: Record<string, any>) => {
            return {
              userId: rec.id,
              username: rec.username,
              fullName: `${rec.name} ${rec.surname}`,
              followedUsers: rec.followed_users,
              phoneContactsUsers: rec.app_contacts,
              languages: rec.languages,
              recommended_for_following_priority:
                rec.recommended_for_following_priority,
              state: rec.state,
              isTester: rec.is_tester,
            }
          }),
        )
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
