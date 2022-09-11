import dotenv from 'dotenv'

import logger from '../utils/Logger'
import {MessageBroker} from './MessageBroker'

dotenv.config()
const amqp = new MessageBroker()
amqp
  .connect()
  .then((broker) => {
    broker
      .send(
        {
          name: 'matching',
          type: 'fanout',
          routingKey: '',
        },
        {
          id: 'userContactsUpdated',
          data: {
            ownerId: 3268,
            userIds: [3243],
          },
        },
      )
      .catch((e) => logger.debug(e))
  })
  .catch((e) => logger.debug(e))
