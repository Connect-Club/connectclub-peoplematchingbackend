import neo4jClient from '../dao/neo4jClient'
import postgresClient from '../dao/postgresClient'
import {sleep} from '../utils/common'
import logger from '../utils/Logger'

export class UserGoalsImportJob {
  async doImport(): Promise<void> {
    // do something
    logger.debug('starting import user goals from PG')
    const users = await neo4jClient.getUserIdsWithLastSync()
    let imported = 0
    for (const user of users) {
      const goals = await postgresClient.getUserGoals(user.userId)
      if (goals.length === 0) {
        continue
      }
      imported += goals.length
      const doMerge = async () => {
        return await neo4jClient.mergeUserTags(
          user.userId,
          goals.map((i) => {
            return {
              score: 15,
              id: i.goal_id.toString(),
              category: 'goals',
              interestName: i.goal_name,
              categoryName: 'Selected Goal',
            }
          }),
          'goals_layer',
        )
      }
      let retries = 0
      while (!(await doMerge())) {
        if (retries === 4) {
          logger.error(
            'NEO4J users general goals merge failed, reties exhausted!',
          )
          break
        }
        retries++
        logger.warn('NEO4J users general goals merge failed, retrying', retries)
        await sleep(1000 * retries)
        await doMerge()
      }
    }
    logger.debug(
      'Import of users goals finished. Total imported rows',
      imported,
    )
  }
}
