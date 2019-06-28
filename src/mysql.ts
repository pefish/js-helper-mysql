import '@pefish/js-node-assist'
import FileUtil from '@pefish/js-util-file'
import * as path from 'path'
import ErrorHelper from '@pefish/js-error'
import ObjectUtil from '@pefish/js-util-object'
import AssertUtil from '@pefish/js-util-assert'
import Sequelize from 'sequelize'

class SequelizeHelper {

  _models: object
  _modelPath: string
  _mysqlConfig: object
  sequelize: any

  constructor (mysqlConfig: object, modelPath: string = null) {
    this._models = {}
    this._modelPath = modelPath
    this._mysqlConfig = mysqlConfig
    this.sequelize = null
  }

  /**
   * 加载models
   */
  async init (dbType: string = 'mysql'): Promise<void> {
    if (dbType === 'mysql') {
      this.sequelize = new Sequelize(this._mysqlConfig['database'], this._mysqlConfig['username'], this._mysqlConfig['password'], {
        host: this._mysqlConfig['host'],
        port: this._mysqlConfig['port'] || 3306,
        dialect: 'mysql',
        pool: {
          max: 30,
          min: 0,
          idle: 10000,
          acquire: 30000
        },
        define: {
          timestamps: true,
          underscored: true,
          paranoid: false,
          freezeTableName: true
        },
        operatorsAliases: Sequelize.Op && Sequelize.Op.Aliases,
        logging: (sql) => {
          // global[`debug`] && logger.info(sql)
        },
        dialectOptions: {
          multipleStatements: true,
          // useUTC: true  // 设置读到的时间的时区
        },
        timezone: '+00:00' // 设置写入的时间的时区
      })
      global.logger.info(`连接mysql: ${this._mysqlConfig['host']} 中...`)
      await this.sequelize.authenticate()
      global.logger.info(`mysql: ${this._mysqlConfig['host']} 连接成功`)
    } else if (dbType === 'sqlite') {
      this.sequelize = new Sequelize(this._mysqlConfig['database'], null, null, {
        dialect: 'sqlite',
        storage: this._mysqlConfig['filename'],
        logging: (sql) => {
          // global[`debug`] && logger.info(sql)
        }
      })
      global.logger.info(`连接sqlite: ${this._mysqlConfig['filename']} 中...`)
      await this.sequelize.authenticate()
      global.logger.info(`sqlite: ${this._mysqlConfig['filename']} 连接成功`)
    } else {
      throw new ErrorHelper(`dbType 有误。dbType: ${dbType}`)
    }
    this._modelPath && this._geneModels()
  }

  _geneModels (): object {
    const filesAndDirs = FileUtil.getFilesAndDirs(this._modelPath)
    for (const file of filesAndDirs['files'].values()) {
      const modelName = file.substr(0, file.length - 3).toLowerCase()
      this._models[modelName] = (require(path.resolve(`${this._modelPath}/${file}`)).default)(this)
      Object.assign(this._models[modelName], this._modelMethods(this._models[modelName]))
    }
    return this._models
  }

  /**
   * 根据模型同步建立表结构
   * @param isForce
   * @returns {Promise<boolean>}
   */
  async sync (isForce: boolean): Promise<boolean> {
    for (const model of Object.values(this._geneModels())) {
      await model.sync(isForce)
    }
    return true
  }

  async query (sql: string, opt: object = null): Promise<any> {
    return opt ? this.sequelize.query(sql, opt) : this.sequelize.query(sql)
  }

  regularString (str: string | number): string {
    return str.toString().replace(/\\/g, '\\\\').replace(/\'/g, '\\\'')
  }

  /**
   * 执行脚本文件
   * @param filename {string} 绝对路径
   * @returns {Promise<void>}
   */
  async executeSqlFile (filename: string): Promise<any> {
    const sql = `BEGIN;${FileUtil.readSync(filename).toString()}COMMIT;`
    global.logger.info(`[sql] ${sql}`)
    return await this.sequelize.query(sql, {
      raw: true
    })
  }

  async executeSql (sql: string): Promise<any> {
    const sql1 = `BEGIN;${sql}COMMIT;`
    global.logger.info(`[sql] ${sql1}`)
    return await this.sequelize.query(sql1, {
      raw: true
    })
  }

  /**
   * 开启事务
   * @returns {Promise<*|Transaction>}
   */
  async begin (): Promise<any> {
    const transaction = new this.sequelize.Transaction(this.sequelize)
    global.logger.info(`[sql] [transactionId: ${transaction.id}] begin`)
    await transaction.prepareEnvironment()
    return transaction
  }

  /**
   * 提交事务。支持多次调用
   * @param transaction {Transaction} 事务实例
   * @returns {Promise<Promise<*>|Promise|*|Object>}
   */
  async commit (transaction: any): Promise<any> {
    if (!transaction.finished) {
      global.logger.info(`[sql] [transactionId: ${transaction.id}] commit`)
      return await transaction.commit()
    }
  }

  /**
   * 回滚事务。支持多次调用
   * @param transaction {Transaction} 事务实例
   * @returns {Promise<T>}
   */
  async rollback (transaction: any): Promise<any> {
    if (!transaction.finished) {
      global.logger.info(`[sql] [transactionId: ${transaction.id}] rollback`)
      return await transaction.rollback().catch(() => {})
    }
  }

  /**
   * 使用select sql
   * @param sql {String} sql
   * @param replacements {Object} 替换参数, default: {}
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>}
   */
  async selectBySql (sql: string, replacements: object = {}, transaction: any = null): Promise<Array<any>> {
    const opt = {
      type: this.sequelize.QueryTypes.SELECT,
      replacements: replacements
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.debug(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    const results = await this.query(sql, opt)
    if (!results || results.length <= 0) {
      return []
    }
    return results
  }

  /**
   * 使用create sql,创建数据库、表
   * @param sql {String} sql
   * @param replacements {Object} 替换参数, default: {}
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<boolean>}
   */
  async createBySql (sql: string, replacements: object = {}, transaction: any = null): Promise<boolean> {
    const opt = {
      type: this.sequelize.QueryTypes.UPDATE,
      replacements: replacements
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.info(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    await this.query(sql, opt)
    return true
  }

  /**
   * 创建数据库
   * @param databaseName
   * @param force 存在就drop掉
   * @returns {Promise<*>}
   */
  async createDatabase (databaseName: string, force: boolean = false): Promise<any> {
    try {
      await this.dropDatabase(databaseName)
    } catch (err) {

    }
    const sql = `create database ${databaseName}`
    global.logger.info(`[sql] ${sql}`)
    return await this.query(sql)
  }

  async dropDatabase (databaseName: string): Promise<any> {
    const sql = `drop database ${databaseName}`
    global.logger.info(`[sql] ${sql}`)
    return await this.query(sql)
  }

  /**
   * select查询
   * @param opts {Object} 查询参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>}
   */
  async select (opts: object, transaction: any = null): Promise<Array<any>> {
    // opts = {
    //   select: ['*'],
    //   from: '',
    //   where: {},
    //   order: [],
    //   limit: [0, 1],
    //   forUpdate: true
    //   if: true
    // }
    if (opts['if'] !== undefined && opts['if'] !== true && opts['if']() !== true) {
      return []
    }
    // sql
    const sql = `
      select
        ${opts['select']}
      from
        ${opts['from']}
      ${await this._assembleParam('where', opts['where'])}
      ${await this._assembleParam('order', opts['order'])}
      ${await this._assembleParam('limit', opts['limit'])}
      ${await this._assembleParam('groupBy', opts['groupby'])}
      ${await this._assembleParam('forUpdate', opts['forUpdate'])}
    `
    const opt = {
      type: this.sequelize.QueryTypes.SELECT,
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.debug(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    const results = await this.query(sql, opt)
    if (!results || results.length <= 0) {
      return []
    }
    return results
  }

  /**
   * select查询
   * @param opts {Object} 查询参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>}
   */
  async selectOne (opts: object, transaction: any = null): Promise<any> {
    // opts = {
    //   select: ['*'],
    //   from: '',
    //   where: {},
    //   order: [],
    //   limit: [0, 1],
    //   forUpdate: true
    //   if: true
    // }
    const results = await this.select(opts, transaction)
    if (results.length === 0) {
      return null
    }
    return results[0]
  }

  /**
   * 求和
   * @param opts {Object} 参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>}
   */
  async sum (opts: object, transaction: any = null): Promise<number> {
    // opts = {
    //   sum: '',
    //   from: '',
    //   where: {},
    // }
    if (opts['if'] !== undefined && opts['if'] !== true && opts['if']() !== true) {
      return 0
    }
    // sql
    const sql = `
      select
        sum(${opts['sum']}) as sum
      from
        ${opts['from']}
      ${await this._assembleParam('where', opts['where'])}
    `
    const opt = {
      type: this.sequelize.QueryTypes.SELECT,
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.debug(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    const results = await this.query(sql, opt)
    if (!results || results.length <= 0) {
      return 0
    }
    return results[0]['sum'].toString().toNumber_() || 0
  }

  /**
   * 关闭数据库连接
   * @returns {Promise<void>}
   */
  async close (): Promise<any> {
    return this.sequelize && this.sequelize.close()
  }

  /**
   * 求总数
   * @param opts {Object} 参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>}
   */
  async count (opts: object, transaction: any = null): Promise<number> {
    // opts = {
    //   from: '',
    //   where: {}
    // }
    if (opts['if'] !== undefined && opts['if'] !== true && opts['if']() !== true) {
      return 0
    }
    // sql
    const sql = `
      select
        count(*) as count
      from
        ${opts['from']}
      ${await this._assembleParam('where', opts['where'])}
    `
    const opt = {
      type: this.sequelize.QueryTypes.SELECT,
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.debug(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    const results = await this.query(sql, opt)
    if (!results || results.length <= 0) {
      return 0
    }
    return results[0]['count']
  }

  async _assembleParam (name: string, data: any): Promise<any> {
    switch (name) {
      case 'select':
        let select = ''
        if (typeof data === 'string') {
          return data
        }
        if (!data || data.length <= 0) {
          select = '*'
        } else {
          select = data.map((val) => {
            return val
          }).join(',')
        }
        return select
      case 'where':
        let where = ''
        if (data) {
          if (typeof data === 'string') {
            return data
          }
          where = where + 'where 1 = 1 '
          for (const [key, value] of Object.entries(data)) {
            if (value !== null && value !== undefined) {
              if (key === '$or') {
                where += 'and ( 1 = 2 '
                for (const [key1, value1] of Object.entries(value)) {
                  if (value1 instanceof Array) {
                    where += `or ${key1} ${value1[0]} ${value1[1]} `
                  } else {
                    if (value1 === 'null') {
                      where += `or ${key1} is null `
                    } else {
                      where += `or ${key1} = '${this.regularString(value1)}' `
                    }
                  }
                }
                where += ')'
              } else {
                if (value instanceof Array) {
                  where += `and ${key} ${value[0]} ${value[1]} `
                } else if (value === 'null') {
                  where += `and ${key} is null `
                } else if (AssertUtil.isType(value, 'string', {}, false) || AssertUtil.isType(value, 'number', {}, false)) {
                  where += `and ${key} = '${this.regularString(value as string | number)}' `
                } else {
                  // 对象
                  for (const [cond, target] of Object.entries(value)) {
                    if (target !== undefined && target !== null) {
                      where += `and ${key} ${cond} ${target} `
                    }
                  }
                }
              }
            }
          }
        }
        return where
      case 'order':
        let order = ''
        if (data) {
          if (typeof data === 'string') {
            return 'order by ' + data
          }
          order = order + 'order by ' + data.map((val) => {
            return `${val[0]} ${val[1]}`
          }).join(',')
        }
        return order
      case 'limit':
        let limit = ''
        if (data) {
          if (typeof data === 'string') {
            return `limit ${data}`
          }
          limit = `limit ${data[0]}, ${data[1]}`
        }
        return limit
      case 'forUpdate':
        let forUpdate = ''
        if (data === true) {
          forUpdate = 'for update'
        }
        return forUpdate
      case 'groupBy':
        let groupBy = ''
        if (data) {
          groupBy = 'group by ' + data
        }
        return groupBy
      case 'update':
        let update = ''
        if (data) {
          for (const [key, value] of Object.entries(data)) {
            if (value !== undefined && value !== null) {
              update = update + `${key} = '${this.regularString(value as string | number)}', `
            }
          }
          update.length > 0 && (update = update.substring(0, update.length - 2) + ' ')
        }
        return update
      case 'insert':
        let insert = ''
        if (data) {
          data = ObjectUtil.removeEmpty(data)
          const fields = Object.keys(data).join(','), values = Object.values(data).map(val => `'${this.regularString(val as string | number)}'`).join(',')
          insert = `(${fields}) values (${values})`
        }
        return insert
      case 'batchInsert':
        let batchInsert = ''
        if (data) {
          const fields = data[0].join(',')
          const values = data[1].map((val) => {
            return `(${val.map(a => {
              if (!a) {
                return 'null'
              }
              return `'${this.regularString(a)}'`
            }).join(',')})`
          }).join(',')
          batchInsert = `(${fields}) values ${values}`
        }
        return batchInsert
      default:
        return null
    }
  }

  /**
   * 联表查询
   * @param opts {Object} 参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>}
   */
  async unionSelect (opts: object, transaction: any = null): Promise<Array<any>> {
    // opts = {
    //   from: '',
    //   to: '',
    //   unionType: 'left join'
    //   where: {},
    //   order: [],
    //   limit: [0, 1],
    //   select: [],
    //   on: ['id', 'depositwithdraw_id'],
    //   forUpdate: true
    // }
    if (opts['if'] !== undefined && opts['if'] !== true && opts['if']() !== true) {
      return []
    }
    if (!opts['from'] || !opts['to'] || !opts['on'] || !opts['unionType']) {
      throw new ErrorHelper(`params error`)
    }
    // select
    const select = await this._assembleParam('select', opts['select'])
    // on
    const on = `${opts['from']}.${opts['on'][0]} = ${opts['to']}.${opts['on'][1]}`
    // where
    const where = await this._assembleParam('where', opts['where'])
    // order
    const order = await this._assembleParam('order', opts['order'])
    // limit
    const limit = await this._assembleParam('limit', opts['limit'])
    // forUpdate
    const forUpdate = await this._assembleParam('forUpdate', opts['forUpdate'])
    // sql
    const sql = `
      select
        ${select}
      from
        ${opts['from']}
      ${opts['unionType']}
        ${opts['to']}
      on
        ${on}
      ${where}
      ${order}
      ${limit}
      ${forUpdate}
    `
    const opt = {
      type: this.sequelize.QueryTypes.SELECT,
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.debug(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    const results = await this.query(sql, opt)
    if (!results || results.length <= 0) {
      return []
    }
    return results
  }

  /**
   * update操作
   * @param opts {Object} 参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>}
   */
  async update (opts: object, transaction: any = null): Promise<any> {
    // opts = {
    //   update: {},
    //   from: '',
    //   where: {}
    // }
    if (opts['if'] !== undefined && opts['if'] !== true && opts['if']() !== true) {
      return
    }
    // sql
    const sql = `
      update
        ${opts['from']}
      set
        ${await this._assembleParam('update', opts['update'])}
      ${await this._assembleParam('where', opts['where'])}
    `
    const opt = {
      type: this.sequelize.QueryTypes.UPDATE,
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.info(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    return await this.query(sql, opt)
  }

  async delete (opts: object, transaction: any = null): Promise<any> {
    // opts = {
    //   from: '',
    //   where: {}
    // }
    if (opts['if'] !== undefined && opts['if'] !== true && opts['if']() !== true) {
      return
    }
    // sql
    const sql = `
      delete from
        ${opts['from']}
      ${await this._assembleParam('where', opts['where'])}
    `
    const opt = {
      type: this.sequelize.QueryTypes.DELETE,
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.info(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    return await this.query(sql, opt)
  }

  /**
   * 更新数据，没有就插入，具有排他性
   * @param opts {Object} 参数
   * @param transaction {Transaction} 事务实例, default: null, 不传的话默认开启事务保证排他性
   * @returns {Promise<*>} id
   */
  async updateOrInsert (opts: object, transaction: any = null): Promise<any> {
    // opts = {
    //   updateOrInsert: {},
    //   from: '',
    //   where: {}
    // }
    transaction || (transaction = await this.begin())
    if (opts['if'] !== undefined && opts['if'] !== true && opts['if']() !== true) {
      return
    }
    try {
      const selectResult = await this.selectOne({
        select: 'id',
        from: opts['from'],
        where: opts['where']
      }, transaction)
      if (!selectResult) {
        // insert
        Object.assign(opts['updateOrInsert'], opts['where'])
        const insertResult = await this.insert({
          insert: opts['updateOrInsert'],
          from: opts['from'],
        }, transaction)
        await this.commit(transaction)
        return insertResult[0]
      } else {
        await this.update({
          update: opts['updateOrInsert'],
          from: opts['from'],
          where: opts['where']
        }, transaction)
        await this.commit(transaction)
        return selectResult['id']
      }
    } catch (err) {
      await this.rollback(transaction)
      throw err
    }
  }

  /**
   * 插入数据
   * @param opts {Object} 参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>} [id, 影响条数]
   */
  async insert (opts: object, transaction: any = null): Promise<any> {
    // opts = {
    //   insert: {},
    //   from: '',
    //   returnResult: true
    // }
    if (opts['if'] !== undefined && opts['if'] !== true && opts['if']() !== true) {
      return
    }
    // sql
    const sql = `
      insert into
        ${opts['from']}
      ${await this._assembleParam('insert', opts['insert'])}
    `
    const opt = {
      type: this.sequelize.QueryTypes.INSERT,
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.info(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    const insertResult = await this.query(sql, opt)
    if (opts['returnResult'] === true) {
      const sql1 = `select * from ${opts['from']} where id = ${insertResult[0]} limit 0, 1`
      global.logger.info(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql1}`)
      return (await this.query(sql1, {
        type: this.sequelize.QueryTypes.SELECT,
        transaction: transaction
      }))[0]
    } else {
      return insertResult
    }
  }

  /**
   * 批量插入
   * @param opts {Object} 参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>}
   */
  async batchInsert (opts: object, transaction: any = null): Promise<any> {
    // opts = {
    //   batchInsert: [['name', 'age'], [[], [], []]],
    //   from: '',
    //   if: true
    // }
    if (opts['if'] !== undefined && opts['if'] !== true && opts['if']() !== true) {
      return []
    }
    // sql
    const sql = `
      insert into
        ${opts['from']}
      ${await this._assembleParam('batchInsert', opts['batchInsert'])}
    `
    const opt = {
      type: this.sequelize.QueryTypes.INSERT,
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.info(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    return await this.query(sql, opt)
  }

  async startTransaction (fun: (tran: any) => Promise<void>): Promise<void> {
    const tran = await this.begin()
    try {
      await fun(tran)
      await this.commit(tran)
    } catch (err) {
      await this.rollback(tran)
      throw err
    }
  }

  /**
   * 使用update sql
   * @param sql {String} sql
   * @param replacements {Object} 替换参数, default: {}
   * @param transaction {Transaction} 事务实例
   * @returns {Promise<*>}
   */
  async updateBySql (sql: string, replacements: object = {}, transaction: any = null): Promise<any> {
    const opt = {
      type: this.sequelize.QueryTypes.UPDATE,
      replacements: replacements
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.info(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    return await this.query(sql, opt)
  }

  async deleteBySql (sql: string, replacements: object = {}, transaction: any = null): Promise<any> {
    const opt = {
      type: this.sequelize.QueryTypes.DELETE,
      replacements: replacements
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.info(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    return await this.query(sql, opt)
  }

  /**
   * 使用insert sql
   * @param sql {String} sql
   * @param replacements {Object} 替换参数, default: {}
   * @param transaction {Transaction} 事务实例
   * @returns {Promise<*>}
   */
  async insertBySql (sql: string, replacements: object = {}, transaction: any = null): Promise<any> {
    const opt = {
      type: this.sequelize.QueryTypes.INSERT,
      replacements: replacements
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.info(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    return await this.query(sql, opt)
  }

  getModel (modelName: string): any {
    return this._models[modelName.toLowerCase()]
  }

  _modelMethods (model: any): object {
    // 会覆写sequelize model中的方法
    return {
      findLatestOne_: async (condition = {}) => {
        return model.findOne({
          where: condition,
          order: [['updated_at', 'DESC']],
        })
      },
      insert_: async (obj) => {
        return model.create(obj)
      },
      updateInsert_: async (updateObj, condition) => {
        const result = await model.findOne({
          where: condition
        })
        if (result) {
          await result.update(updateObj)
        } else {
          await model.create(Object.assign(updateObj, condition))
        }
        return true
      },
      delete_: async (condition) => {
        const temp = await model.findOne({
          where: condition
        })
        if (!temp) {
          throw new ErrorHelper(`target not found`)
        }
        return temp.destroy()
      },
      update_: async (update, where) => {
        const temp = await model.findOne({
          where: where
        })
        if (!temp) {
          throw new ErrorHelper(`target not found`)
        }
        await temp.update(update)
        return true
      }
    }
  }
}

export default SequelizeHelper
