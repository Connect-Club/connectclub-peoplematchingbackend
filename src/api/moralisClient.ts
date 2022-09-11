import axios from 'axios'
import {EventEmitter} from 'eventemitter3'

import {sleep} from '../utils/common'
import logger from '../utils/Logger'

const baseUrl = 'https://deep-index.moralis.io/api/v2/'
const API_KEY =
  process.env.MORALIS_KEY ||
  'g1YjW5BRYpJ6r0ghoa9jNpBl4psZwITanU4vAzsgVVmPPBWr4AbIcz9rDSCxmNNP'

enum RequestMethods {
  GET = 'GET',
  POST = 'POST',
  PUT = 'PUT',
  DELETE = 'DELETE',
}

const DEFAULT_WEIGHT = 5
const emitter = new EventEmitter()

export class MoralisClient {
  private usedLimit = 0
  private HARD_LIMIT = 20
  private priorityQueue: Array<() => Promise<any>> = []
  private generalQueue: Array<() => Promise<any>> = []

  constructor() {
    this.execJob = this.execJob.bind(this)
    emitter.on('queuesUpdated', this.execJob)
  }

  private async execJob(): Promise<void> {
    if (this.priorityQueue.length === 0 && this.generalQueue.length === 0) {
      return
    }
    logger.debug('Exec job')
    if (this.usedLimit + DEFAULT_WEIGHT >= this.HARD_LIMIT) {
      const timeout =
        1000 + ((this.usedLimit + DEFAULT_WEIGHT) / this.HARD_LIMIT) * 1000
      logger.debug(
        'Setting timeout due to ',
        this.usedLimit + DEFAULT_WEIGHT,
        'bigger than or equal to',
        this.HARD_LIMIT,
        JSON.stringify(this.priorityQueue),
        JSON.stringify(this.generalQueue),
      )
      setTimeout(() => {
        emitter.emit('queuesUpdated')
      }, timeout)
      return
    }
    if (this.priorityQueue.length > 0) {
      const job = this.priorityQueue.shift()
      logger.debug('Pop priority job', job)
      if (job) {
        await job()
        emitter.emit('queuesUpdated')
        return
      }
    } else if (this.generalQueue.length > 0) {
      const job = this.generalQueue.shift()
      logger.debug('Pop general job', job)
      if (job) {
        await job()
        emitter.emit('queuesUpdated')
        return
      }
    }
  }

  private async enqueueOrRun(
    req: (...p: any) => Promise<any>,
    priority: boolean,
    ...params: any
  ): Promise<any> {
    if (priority) {
      const _req = req.bind(this)
      const promise = new Promise((res) => {
        return res(_req(params))
      })
      this.priorityQueue.push((): Promise<any> => {
        return promise
      })
      logger.debug('Enqueued priority job', JSON.stringify(params))
      emitter.emit('queuesUpdated')
      return promise
    } else {
      const _req = req.bind(this)
      const promise = new Promise((res) => {
        return res(_req(params))
      })
      this.generalQueue.push((): Promise<any> => {
        return promise
      })
      logger.debug('Enqueued general job', JSON.stringify(params))
      emitter.emit('queuesUpdated')
      return promise
    }
  }

  async _getNftsByWallet(
    address: string,
    highPriority: boolean,
  ): Promise<Array<Record<string, any>>> {
    const req = {
      method: RequestMethods.GET,
      baseURL: baseUrl,
      headers: {
        accept: 'application/json',
        'X-API-Key': API_KEY,
      },
      params: {
        chain: process.env.NODE_ENV === 'production' ? 'eth' : 'goerli',
        limit: 100,
      },
    }
    const _doReq = async (
      request: Record<string, any>,
      allNfts: Array<Record<string, any>> = [],
      retries = 0,
    ): Promise<Array<Record<string, any>>> => {
      try {
        const response = await axios.get<Record<string, any>>(
          `${address}/nft`,
          request,
        )
        this.HARD_LIMIT = Number.parseInt(
          response.headers['x-rate-limit-limit'],
        )
        this.usedLimit = Number.parseInt(response.headers['x-rate-limit-used'])
        if (!response.data) {
          return allNfts
        }
        const results = response.data.result.filter(
          (r: Record<string, any>) => {
            return r.token_address !== undefined
          },
        )
        for (const r of results) {
          r.totalCount = await this.getNftOwnersCount(
            r.token_address,
            highPriority,
          )
        }
        allNfts = allNfts.concat(
          results.map((nft: Record<string, any>) => {
            return {
              token_id: nft.token_id,
              token_address: nft.token_address,
              contract_type: nft.contract_type,
              name: nft.name,
              symbol: nft.symbol,
              metadata: nft.metadata,
              totalCount: nft.totalCount,
            }
          }),
        )
        // if (response.data.cursor) {
        //   request.params = {...req.params, cursor: response.data.cursor}
        //   return _doReq(request, allNfts)
        // }
        return allNfts
      } catch (err: any) {
        if (err.response?.status === 429) {
          return this.enqueueOrRun(this._getNftsByWallet, highPriority, address)
        }
        logger.error(
          'NFT get list failed for',
          address,
          err.message,
          `retry #${retries}`,
          err.response,
        )
        if (retries < 5) {
          await sleep(500 * retries + 1)
          return _doReq(request, allNfts, retries + 1)
        } else {
          logger.warn('Retry attempts exhausted for getting NFTs')
          return allNfts
        }
      }
    }
    return await _doReq(req)
  }

  async getNftsByWallet(
    address: string,
    highPriority = true,
  ): Promise<Array<Record<string, any>>> {
    return this.enqueueOrRun(this._getNftsByWallet, highPriority, address)
  }

  private async _getNftOwnersCount(
    tokenAddress: string,
    highPriority: boolean,
  ): Promise<number> {
    const req = {
      method: RequestMethods.GET,
      baseURL: baseUrl,
      headers: {
        accept: 'application/json',
        'X-API-Key': API_KEY,
      },
      params: {
        chain: process.env.NODE_ENV === 'production' ? 'eth' : 'goerli',
        cursor: '',
      },
    }
    const _doReq = async (
      tokenAddress: string,
      request: Record<string, any>,
      retries = 0,
    ): Promise<number> => {
      try {
        const response = await axios.get<Record<string, any>>(
          `nft/${tokenAddress}/owners`,
          request,
        )
        this.usedLimit = Number.parseInt(response.headers['x-rate-limit-used'])
        if (!response.data) {
          return -1
        }
        return response.data.total
      } catch (err: any) {
        if (err.response?.status === 429) {
          return this.enqueueOrRun(
            this._getNftOwnersCount,
            highPriority,
            tokenAddress,
          )
        }
        logger.error('NFT get owners failed', err.message, 'retry #', retries)
        if (retries < 5) {
          await sleep(500 * retries + 1)
          return _doReq(tokenAddress, request, retries + 1)
        } else {
          logger.warn('Retry attempts exhausted for getting NFT owners')
          return -1
        }
      }
    }
    return _doReq(tokenAddress, req)
  }

  async getNftOwnersCount(
    tokenAddress: string,
    highPriority = true,
  ): Promise<number> {
    return this.enqueueOrRun(
      this._getNftOwnersCount,
      highPriority,
      tokenAddress,
    )
  }
}

const moralisClient = new MoralisClient()
export default moralisClient
