import neo4jClient from '../dao/neo4jClient'
import postgresClient from '../dao/postgresClient'
import {sleep} from '../utils/common'
import logger from '../utils/Logger'

export class UserIndustriesImportJob {
  async doImport(): Promise<void> {
    // do something
    logger.debug('starting import user industries from PG')
    const users = await neo4jClient.getUserIdsWithLastSync()
    let imported = 0
    for (const user of users) {
      const industries = await postgresClient.getUserIndustries(user.userId)
      if (industries.length === 0) {
        continue
      }
      imported += industries.length
      const doMerge = async () => {
        return await neo4jClient.mergeUserTags(
          user.userId,
          industries.map((i) => {
            return {
              score: 55,
              id: i.industry_id.toString(),
              category: 'industry',
              interestName: i.industry_name,
              categoryName: 'Selected Industry',
            }
          }),
          'professional_layer',
        )
      }
      let retries = 0
      while (!(await doMerge())) {
        if (retries === 4) {
          logger.error(
            'NEO4J users general industries merge failed, reties exhausted!',
          )
          break
        }
        retries++
        logger.warn(
          'NEO4J users general industries merge failed, retrying',
          retries,
        )
        await sleep(1000 * retries)
        await doMerge()
      }
    }
    logger.debug(
      'Import of users industries finished. Total imported rows',
      imported,
    )
  }
}
