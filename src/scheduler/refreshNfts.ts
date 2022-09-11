import moment from 'moment'

import moralisClient from '../api/moralisClient'
import neo4jClient from '../dao/neo4jClient'
import logger from '../utils/Logger'

export default async (): Promise<void> => {
  logger.debug('Starting NFT sync job')
  try {
    const now = moment().utc().unix()
    const users = await neo4jClient.getUsersHavingWallet(now, 500)
    for (const user of users) {
      const tokens = await moralisClient.getNftsByWallet(user.wallet, false)
      logger.debug('Done refreshing NFTs. Total', tokens.length)
      await neo4jClient.mergeUserWallet(
        user.userId,
        user.wallet,
        moment().utc().unix(),
      )
      if (tokens.length > 0) {
        await neo4jClient.mergeUserNfts(user.userId, tokens)
      }
    }
  } catch (err: any) {
    console.error(err)
  }
}
