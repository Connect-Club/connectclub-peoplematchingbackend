import moment from 'moment'

import {MoralisClient} from '../api/moralisClient'
import neo4jClient from '../dao/neo4jClient'
import postgresClient from '../dao/postgresClient'
import {sleep} from '../utils/common'
import logger from '../utils/Logger'

export class UserWalletsImportJob {
  async doImport(): Promise<void> {
    let imported = 0
    logger.debug('starting import user wallets from PG')
    const moralisClient = new MoralisClient()
    const wallets = await postgresClient.getUsersWithWallet()
    for (const wallet of wallets) {
      try {
        // console.log(wallet.id, moment().format('mm:ss:sss'))
        const tokens = await moralisClient.getNftsByWallet(wallet.wallet)
        logger.debug(
          '########### GOT TOKENS ############',
          JSON.stringify(tokens),
        )
        await neo4jClient.mergeUserWallet(
          wallet.id,
          wallet.wallet,
          moment().utc().unix(),
        )
        if (tokens.length > 0) {
          await neo4jClient.mergeUserNfts(wallet.id, tokens)
          imported++
        }
        // await sleep(250)
      } catch (err) {
        logger.error('Error in NFTs import', err)
      }
    }
    logger.debug(
      'Import of users goals finished. Total imported rows',
      imported,
    )
  }
}
