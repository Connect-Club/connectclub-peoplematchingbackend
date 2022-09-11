import neo4jClient from '../dao/neo4jClient'
import postgresClient from '../dao/postgresClient'
import {sleep} from '../utils/common'
import logger from '../utils/Logger'

export class UserScheduledEventsImportJob {
  async doImport(): Promise<void> {
    // do something
    logger.debug('starting import user scheduled events from PG')
    await postgresClient.streamUserScheduledEvents(async (rows: Array<any>) => {
      logger.debug('got new rows from PG', rows.length)
      const doMerge = async () => {
        return await neo4jClient.mergeUserEventSchedule(rows)
      }
      let retries = 0
      while (!(await doMerge())) {
        if (retries === 4) {
          logger.error(
            'NEO4J user scheduled events merge failed, reties exhausted!',
          )
          break
        }
        retries++
        logger.warn(
          'NEO4J user scheduled events merge failed, retrying',
          retries,
        )
        await sleep(1000 * retries)
        await doMerge()
      }
    })
  }
}
