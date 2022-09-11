import * as amqp from 'amqplib'
import {ConsumeMessage} from 'amqplib'

import logger from '../utils/Logger'

export class MessageBroker {
  protected channel: amqp.Channel | undefined
  protected url: string

  constructor(amqpUrl = '') {
    this.url =
      amqpUrl ||
      process.env.MESSENGER_TRANSPORT_DSN ||
      'amqp://stage:1719h735O1qZtSs@10.196.0.32:5672/%2f' ||
      'amqp://localhost'
  }

  async connect(): Promise<MessageBroker> {
    console.log('Connecting to ', this.url)
    const connection = await amqp.connect(this.url)
    this.channel = await connection.createChannel()
    await this.channel.prefetch(50) // limit simultaneous deliveries
    this.channel.on('error', (e) => {
      logger.error(e)
      throw e
    })
    return this
  }

  async checkExchange(exchange: string): Promise<boolean> {
    let isExchangeExists = false
    if (this.channel) {
      try {
        await this.channel.checkExchange(exchange)
        isExchangeExists = true
      } catch (e) {
        // Exchange doesn't exist. Channel will be closed
      }
    }
    return isExchangeExists
  }

  async checkQueue(queue: string): Promise<boolean> {
    let isQueueExists = false
    if (this.channel) {
      try {
        await this.channel.checkQueue(queue)
        isQueueExists = true
      } catch (e) {
        // Queue doesn't exist. Channel will be closed
      }
    }
    return isQueueExists
  }

  async deleteQueue(queue: string): Promise<boolean> {
    let isDeleted = false
    if (this.channel) {
      try {
        await this.channel.deleteQueue(queue)
        isDeleted = true
      } catch (e) {
        // Queue doesn't exist. Channel will be closed
      }
    }
    return isDeleted
  }

  /* Send message to queue */
  async send(
    exchangeOrQueue:
      | {name: string; type: string; routingKey: string}
      | string = '',
    msg: unknown,
    options: Record<string, unknown> = {durable: true},
  ): Promise<void> {
    if (this.channel) {
      const message = JSON.stringify(msg)
      // If exchange is not defined, send to default exchange
      if (typeof exchangeOrQueue === 'string') {
        await this.channel.assertQueue(exchangeOrQueue, options)
        this.channel.sendToQueue(exchangeOrQueue, Buffer.from(message))
      } else {
        // Create exchange and publish it
        await this.channel.assertExchange(
          exchangeOrQueue.name,
          exchangeOrQueue.type,
          options,
        )
        this.channel.publish(
          exchangeOrQueue.name,
          exchangeOrQueue.routingKey,
          Buffer.from(message),
        )
      }
      console.log(" [x] Sent to %s: '%s'", exchangeOrQueue, message)
    }
  }

  async subscribe(
    exchangeOrQueue:
      | {
          name: string
          routingKey: string
          type: string
          queue: string
          options?: Record<string, unknown>
        }
      | string = '',
    callback: (
      msg: ConsumeMessage | null,
      ack: () => void,
      nack: () => void,
    ) => void,
    queueOptions: Record<string, unknown> = {durable: true},
    consumeParams = {noAck: false},
  ): Promise<void> {
    if (this.channel) {
      const isExchange = typeof exchangeOrQueue === 'object'
      const queue = isExchange
        ? (exchangeOrQueue as Record<string, any>).queue
        : exchangeOrQueue

      // Create exchange
      if (isExchange) {
        await this.channel.assertExchange(
          (exchangeOrQueue as Record<string, any>).name,
          (exchangeOrQueue as Record<string, any>).type,
          (exchangeOrQueue as Record<string, any>).options || {durable: true},
        )
      }

      // Create queue
      await this.channel.assertQueue(queue, queueOptions)

      // Bind queue according to routing
      if (isExchange) {
        await this.channel.bindQueue(
          queue,
          (exchangeOrQueue as Record<string, any>).name,
          (exchangeOrQueue as Record<string, any>).routingKey,
        )
      }

      // Consume messages
      await this.channel.consume(
        queue,
        async (msg) => {
          const ack = () => msg && this.channel?.ack(msg)
          const nack = () => msg && this.channel?.nack(msg)
          callback(msg, ack, nack)
        },
        consumeParams,
      )
    }
  }
}
