import * as _ from 'lodash'

import neo4jClient from '../dao/neo4jClient'
import postgresClient from '../dao/postgresClient'
import {BehaviorTag} from '../model/Tag'
import {chunkArray, sleep} from '../utils/common'
import logger from '../utils/Logger'

export class UserEventsParticipantsImportJob {
  async doImport(): Promise<void> {
    // do something
    logger.debug('starting import user events participants from PG')
    const users = await neo4jClient.getUserIdsWithLastSync()
    let imported = 0
    const chunks = chunkArray(users, 100)
    const doMerge = async (
      records: Array<{
        userId: number
        activities: Array<BehaviorTag>
      }>,
    ) => {
      // await sleep(100)
      // logger.debug(JSON.stringify(records))
      // return true
      return await neo4jClient.mergeUserBehaviorActivity(records)
    }
    for (const chunk of chunks) {
      const importRecords: Array<{
        userId: number
        activities: Array<BehaviorTag>
      }> = []
      for (const user of chunk) {
        const rooms = await postgresClient.getUserEventParticipations(
          user.userId,
        )
        if (rooms.length === 0) {
          continue
        }
        imported += rooms.length

        const records = []
        const grouped = _.groupBy(rooms, (rec: Record<string, any>) => {
          return rec.id
        })
        for (const item of Object.keys(grouped)) {
          const grpByStage = _.groupBy(grouped[item], (r) => {
            return r.stage
          })
          for (const item2 of Object.keys(grpByStage)) {
            records.push({
              interests: grpByStage[item2]
                .filter((r) => {
                  return r.interest_id !== null
                })
                .map((r) => {
                  return r.interest_id.toString()
                }),
              id: item,
              category:
                JSON.parse(item2) == true
                  ? 'userMeetingStageJoin'
                  : 'userMeetingParticipate',
              score: JSON.parse(item2) == true ? 3 : 1,
            })
          }
        }
        imported += records.length
        importRecords.push({
          userId: user.userId,
          activities: records,
        })
      }
      let retries = 0
      while (!(await doMerge(importRecords))) {
        if (retries === 4) {
          logger.error(
            'NEO4J users event participants merge failed, reties exhausted!',
          )
          break
        }
        retries++
        logger.warn(
          'NEO4J users event participants merge failed, retrying',
          retries,
        )
        await sleep(1000 * retries)
        await doMerge(importRecords)
      }
    }
    logger.debug(
      'Import of users events participants finished. Total imported rows',
      imported,
    )
  }
}
