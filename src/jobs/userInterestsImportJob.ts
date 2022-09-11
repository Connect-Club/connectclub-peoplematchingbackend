import neo4jClient from '../dao/neo4jClient'
import postgresClient from '../dao/postgresClient'
import {sleep} from '../utils/common'
import logger from '../utils/Logger'

export class UserInterestsImportJob {
  async doImport(): Promise<void> {
    // do something
    logger.debug('starting import user interests from PG')
    const users = await neo4jClient.getUserIdsWithLastSync()
    let imported = 0
    for (const user of users) {
      const interests = await postgresClient.getUserGeneralInterests(
        user.userId,
      )
      if (interests.length === 0) {
        continue
      }
      imported += interests.length
      const doMerge = async () => {
        return await neo4jClient.mergeUserTags(
          user.userId,
          interests.map((i) => {
            return {
              score: 1,
              id: i.interest_id.toString(),
              category: 'gen_interest',
              interestName: i.interest_name,
              categoryName: 'General interest',
            }
          }),
          'general_layer',
        )
      }
      let retries = 0
      while (!(await doMerge())) {
        if (retries === 4) {
          logger.error(
            'NEO4J users general interests merge failed, reties exhausted!',
          )
          break
        }
        retries++
        logger.warn(
          'NEO4J users general interests merge failed, retrying',
          retries,
        )
        await sleep(1000 * retries)
        await doMerge()
      }
    }
    logger.debug(
      'Import of users interests finished. Total imported rows',
      imported,
    )
  }
}
