import cron from 'node-cron'

import refreshNfts from './refreshNfts'

class Scheduler {
  constructor() {}
  start() {
    console.log('Scheduler for refreshing NFTs is launched')
    cron.schedule('*/10 * * * *', async () => {
      await refreshNfts()
    })
  }
}

const scheduler = new Scheduler()
export default scheduler
