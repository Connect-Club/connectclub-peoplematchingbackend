import neo4jClient from '../dao/neo4jClient'
import postgresClient from '../dao/postgresClient'
import {sleep} from '../utils/common'
import logger from '../utils/Logger'

export class UserClubsImportJob {
  async doImport(): Promise<void> {
    // do something
    logger.debug('starting import user clubs from PG')
    const clubs = await postgresClient.getClubsWithEvents()
    await neo4jClient.mergeClubsWithEvents(clubs)
    await postgresClient.streamUserClubs(async (rows: Array<any>) => {
      logger.debug('got new rows from PG', rows.length)
      const doMerge = async () => {
        return await neo4jClient.mergeUserClubs(
          rows.map((rec: Record<string, any>) => {
            return {userId: rec.user_id, clubs: rec.clubs}
          }),
        )
      }
      let retries = 0
      while (!(await doMerge())) {
        if (retries === 4) {
          logger.error('NEO4J user club merge failed, reties exhausted!')
          break
        }
        retries++
        logger.warn('NEO4J user clubs merge failed, retrying', retries)
        await sleep(1000 * retries)
        await doMerge()
      }
    })
  }
}
