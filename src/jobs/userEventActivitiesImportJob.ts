import * as _ from 'lodash'
import moment from 'moment'

import neo4jClient from '../dao/neo4jClient'
import postgresClient from '../dao/postgresClient'
import {BehaviorTag} from '../model/Tag'
import {chunkArray} from '../utils/common'
import logger from '../utils/Logger'

export class UserEventActivitiesImportJob {
  async doImport(): Promise<void> {
    // do something
    logger.debug('starting import user events activities from PG')
    const users = await neo4jClient.getUserIdsWithLastSync()
    let imported = 0
    const chunks = chunkArray(users, 100)
    for (const chunk of chunks) {
      const importRecords: Array<{
        userId: number
        activities: Array<BehaviorTag>
      }> = []
      for (const user of chunk) {
        const activities = await postgresClient.getUserEventActivities(
          user.userId,
          user.lastEventActivitiesSync,
        )
        const lastSync = moment().utc().valueOf()
        if (activities.length === 0) {
          await neo4jClient.updateUserBehaviorLastSync(
            user.userId,
            lastSync,
            'lastEventActivitiesSync',
          )
          continue
        }
        const records = []
        const grouped = _.groupBy(activities, (rec: Record<string, any>) => {
          return rec.id
        })
        for (const item of Object.keys(grouped)) {
          records.push({
            interests: grouped[item]
              .filter((r) => {
                return r.interest_id !== null
              })
              .map((r) => {
                return r.interest_id.toString()
              }),
            id: item,
          })
        }
        imported += records.length
        importRecords.push({
          userId: user.userId,
          activities: records.map((i) => {
            return {
              score: 3,
              id: i.id,
              category: 'userMeetingStageJoin',
              interests: i.interests,
            }
          }),
        })
        await neo4jClient.updateUserBehaviorLastSync(
          user.userId,
          lastSync,
          'lastEventActivitiesSync',
        )
      }
      await neo4jClient.mergeUserBehaviorActivity(importRecords)
    }
    logger.debug(
      'Import of users events activities finished. Total imported rows',
      imported,
    )
  }
}
