import neo4jClient from '../dao/neo4jClient'
import postgresClient from '../dao/postgresClient'
import {sleep} from '../utils/common'
import logger from '../utils/Logger'

export class UserSkillsImportJob {
  async doImport(): Promise<void> {
    // do something
    logger.debug('starting import user skills from PG')
    const users = await neo4jClient.getUserIdsWithLastSync()
    let imported = 0
    for (const user of users) {
      const skills = await postgresClient.getUserSkills(user.userId)
      if (skills.length === 0) {
        continue
      }
      imported += skills.length
      const doMerge = async () => {
        return await neo4jClient.mergeUserTags(
          user.userId,
          skills.map((i) => {
            return {
              score: 5,
              id: i.skill_id.toString(),
              category: 'skills',
              interestName: i.skill_name,
              categoryName: i.group_name,
            }
          }),
          'professional_layer',
        )
      }
      let retries = 0
      while (!(await doMerge())) {
        if (retries === 4) {
          logger.error(
            'NEO4J users general skills merge failed, reties exhausted!',
          )
          break
        }
        retries++
        logger.warn(
          'NEO4J users general skills merge failed, retrying',
          retries,
        )
        await sleep(1000 * retries)
        await doMerge()
      }
    }
    logger.debug(
      'Import of users skills finished. Total imported rows',
      imported,
    )
  }
}
