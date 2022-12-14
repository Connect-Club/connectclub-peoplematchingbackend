import {configure, getLogger} from 'log4js'

configure({
  appenders:
    process.env.NODE_ENV === 'production' || process.env.NODE_ENV === 'stage'
      ? {app: {type: 'console'}}
      : {
          app: {
            type: 'file',
            filename: 'app.log',
          },
        },
  categories: {default: {appenders: ['app'], level: 'debug'}},
})
const logger = getLogger()

export default logger
