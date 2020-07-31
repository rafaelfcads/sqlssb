const { Connection, Request, TYPES } = require('tedious')
const driverSettings = {
  requestTimeout: 0,
  camelCaseColumns: true,
  rowCollectionOnRequestCompletion: true
}

module.exports = class DataAdapter {
  constructor (config) {
    this._config = config
  }
  get isConnection() {
    return this._isConnection
  }
  _connect () {
    const { server, user, password, database, encrypt = false } = this._config
    let connection
    try {
        connection = new Connection({
          server,
          userName: user,
          password,
          options: {
            encrypt,
            database,
            ...driverSettings
          }
        })
  } catch(e) {
    connection = null
  }
    return new Promise((resolve, reject) => {

      if (!connection) return reject('err')
      connection.on('connect', err => {
        if (err) {
          reject(err)
          return
        }
        resolve(connection)
      })

      connection.on('end', () => { 
          console.log('try reconect!')
          setTimeout(() => this.obj.start(this.options),10000)
          
      });
    })
  }
  

  async connect (obj,options) {
      this.obj=obj
      this.options=options
      try{
        this._connection =  await this._connect()
        
      }catch(err){
        console.log('Error to try connection ')
      }
      return this._connection
  }

  receive ({ count = 1, timeout = 5000 } = {}) {

    if (!this._connection) {
      throw new Error('No connection')
    }


    const { queue } = this._config

    const query = `WAITFOR (  
      RECEIVE TOP (@count)
        conversation_group_id,
        conversation_handle,                                                                                                                          
        message_sequence_number,
        CAST(message_body AS VARCHAR(MAX)) as message_body,
        message_type_id,
        message_type_name,
        priority,
        queuing_order,
        service_contract_id,
        service_contract_name,
        service_id,
        service_name,
        status,
        validation
      FROM [${queue}]  
    ), TIMEOUT @timeout`

    return new Promise((resolve, reject) => {
        try{          
          
          const request = new Request(query, (err, rowCount, rows) => {
            
            let [arrRows] = rows != undefined ? rows : []
             
            if (err) {
              reject(err)
              return
            }
            
            if (!arrRows) {
              resolve()
              return
            }

            const response = arrRows.reduce((acc, current) => {
              const key = current.metadata.colName
              acc[key] = current.value
              return acc
            }, {})
  
            response.message_body = response.message_body.toString()
            resolve(response)
          })
          
          request.addParameter('count', TYPES.Int, count)
          request.addParameter('timeout', TYPES.Int, timeout)
          this._connection.execSql(request)

        }catch(error){
          console.log('dataAdapter error')
        }
        
    })
  }

  async send ({ target, type, body, contract, conversationId }) {
    const connection = await this._connect()
    const lines = [`DECLARE @DialogId UNIQUEIDENTIFIER;`]
    const from = this._config.service

    if (conversationId) {
      lines.push(`SET @DialogId = '${conversationId}';`)
    } else {
      lines.push(...[
        `BEGIN DIALOG @DialogId`,
        `FROM SERVICE [${from}] TO SERVICE @target`,
        `ON CONTRACT [${contract}] WITH ENCRYPTION=OFF;`
      ])
    }

    lines.push(...[
      `SEND ON CONVERSATION @DialogId`,
      `MESSAGE TYPE [${type}] (@body);`
    ])

    const query = lines.join('\n')

    return new Promise((resolve, reject) => {
      const request = new Request(query, err => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
        connection.close()
      })

      request.addParameter('body', TYPES.VarChar, body)
      request.addParameter('target', TYPES.VarChar, target)
      connection.execSql(request)
    })
  }

  stop () {
    this._connection.cancel()
    this._connection.close()
  }
}
