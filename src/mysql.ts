import '@pefish/js-node-assist'
import fs from 'fs'
import ErrorHelper from '@pefish/js-error'
import Sequelize from 'sequelize'

interface MysqlConfigration {
  host: string,
  port?: number,
  username: string,
  password: string,
  database: string,
  filename?: string
}

interface SelectOpt {
  select: string | string[],
  from: string,
  where?: string | {
    [x: string]: any
  },
  order?: string | [string, string][],
  limit?: string | number[],
  groupBy?: string | string[],
  forUpdate?: boolean,
  if?: boolean | (() => boolean),
}

interface SumOpt {
  sum: string,
  from: string,
  where?: string | {
    [x: string]: any
  },
  if?: boolean | (() => boolean),
}

interface CountOpt {
  from: string,
  where: string | {
    [x: string]: any
  },
  if?: boolean | (() => boolean),
}

interface UnionSelectOpt {
  from: string,
  to: string,
  unionType: string,
  where?: string | {
    [x: string]: any
  },
  order?: string | [string, string][],
  limit?: string | number[],
  select: string | string[],
  on: string[],
  forUpdate?: boolean,
  if?: boolean | (() => boolean),
}

interface UpdateOpt {
  update: string | {
    [x: string]: any
  },
  from: string,
  where: string | {
    [x: string]: any
  },
  if?: boolean | (() => boolean),
}

interface DeleteOpt {
  from: string,
  where: string | {
    [x: string]: any
  },
  if?: boolean | (() => boolean),
}

interface UpdateOrInsertOpt {
  updateOrInsert: {
    [x: string]: any
  },
  from: string,
  where: string | {
    [x: string]: any
  },
  if?: boolean | (() => boolean),
}

interface InsertOpt {
  insert: {
    [x: string]: any
  },
  from: string,
  if?: boolean | (() => boolean),
}

interface InsertOnDuplicateKeyOpt {
  insert: {
    [x: string]: any
  },
  update: {
    [x: string]: any
  },
  from: string,
  if?: boolean | (() => boolean),
}

interface BatchInsertOpt {
  batchInsert: [string[], string[][]],
  from: string,
  if?: boolean | (() => boolean),
}

class SequelizeHelper {
  private mysqlConfig: MysqlConfigration
  sequelize: any

  constructor (mysqlConfig: MysqlConfigration) {
    this.mysqlConfig = mysqlConfig
    this.sequelize = null
  }

  /**
   * 加载models
   */
  async init (dbType: string = 'mysql'): Promise<void> {
    if (dbType === 'mysql') {
      this.sequelize = new Sequelize(this.mysqlConfig.database, this.mysqlConfig.username, this.mysqlConfig.password, {
        host: this.mysqlConfig.host,
        port: this.mysqlConfig.port || 3306,
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
      global.logger.info(`连接mysql: ${this.mysqlConfig.host} 中...`)
      await this.sequelize.authenticate()
      global.logger.info(`mysql: ${this.mysqlConfig.host} 连接成功`)
    } else if (dbType === 'sqlite') {
      this.sequelize = new Sequelize(this.mysqlConfig.database, null, null, {
        dialect: 'sqlite',
        storage: this.mysqlConfig.filename,
        logging: (sql) => {
          // global[`debug`] && logger.info(sql)
        }
      })
      global.logger.info(`连接sqlite: ${this.mysqlConfig.filename} 中...`)
      await this.sequelize.authenticate()
      global.logger.info(`sqlite: ${this.mysqlConfig.filename} 连接成功`)
    } else {
      throw new ErrorHelper(`dbType 有误。dbType: ${dbType}`)
    }
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
    const sql = `BEGIN;${fs.readFileSync(filename).toString()}COMMIT;`
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
  async selectBySql (sql: string, replacements: object = {}, transaction: any = null): Promise<any[]> {
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
      force === true && await this.dropDatabase(databaseName)
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
  async select (opts: SelectOpt, transaction: any = null): Promise<any[]> {
    if (opts.if !== undefined && opts.if !== true && (opts.if as () => boolean)() !== true) {
      return []
    }
    // sql
    const sql = `
      select
        ${opts.select}
      from
        ${opts.from}
      ${await this._assembleParam('where', opts.where)}
      ${await this._assembleParam('order', opts.order)}
      ${await this._assembleParam('limit', opts.limit)}
      ${await this._assembleParam('groupBy', opts.groupBy)}
      ${await this._assembleParam('forUpdate', opts.forUpdate)}
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
  async selectOne (opts: SelectOpt, transaction: any = null): Promise<any | null> {
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
  async sum (opts: SumOpt, transaction: any = null): Promise<string> {
    if (opts.if !== undefined && opts.if !== true && (opts.if as () => boolean)() !== true) {
      return `0`
    }
    // sql
    const sql = `
      select
        sum(${opts.sum}) as sum
      from
        ${opts.from}
      ${await this._assembleParam('where', opts.where)}
    `
    const opt = {
      type: this.sequelize.QueryTypes.SELECT,
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.debug(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    const results = await this.query(sql, opt)
    if (!results || results.length <= 0 || !results[0]['sum']) {
      return `0`
    }
    return results[0]['sum'].toString() || `0`
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
  async count (opts: CountOpt, transaction: any = null): Promise<number> {
    if (opts.if !== undefined && opts.if !== true && (opts.if as () => boolean)() !== true) {
      return 0
    }
    // sql
    const sql = `
      select
        count(*) as count
      from
        ${opts.from}
      ${await this._assembleParam('where', opts.where)}
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

  async _assembleParam (name: string, data: any): Promise<string> {
    switch (name) {
      case 'select':
        let select = ''
        if (typeof data === 'string') {
          return (data.startsWith(`select`) ? '' : 'select ') + data
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
        if (!data) {
          return where
        }
        if (typeof data === 'string') {
          return (data.startsWith(`where`) ? '' : 'where ') + data
        }
        where = where + 'where 1 = 1 '
        for (let [key, value] of Object.entries(data)) {
          if (value === null || value === undefined) {
            continue
          }
          if ((Object.prototype.toString.call(value) as string).endsWith(`String]`) && (value as string).startsWith(`s:`)) {
            value = (value as string).substring(2)
            where += `and ${(value as string).startsWith(key) ? '' : key} ${value} `
          } else if ((Object.prototype.toString.call(value) as string).endsWith(`String]`) || (Object.prototype.toString.call(value) as string).endsWith(`Number]`)) {
            where += `and ${key} = '${this.regularString(value as string | number)}' `
          }
        }
        return where
      case 'order':
        let order = ''
        if (data) {
          if (typeof data === 'string') {
            return (data.startsWith(`order by`) ? '' : 'order by ') + data
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
            return (data.startsWith(`limit`) ? '' : 'limit ') + data
          }
          limit = `limit ${data[0]}, ${data[1]}`
        }
        return limit
      case 'forUpdate':
        let forUpdate = ''
        if (data) {
          if (typeof data === 'string') {
            return (data.startsWith(`for update`) ? '' : 'for update ') + data
          }
          if (data === true) {
            forUpdate = 'for update'
          }
        }
        return forUpdate
      case 'groupBy':
        const groupBy = ''
        if (data) {
          if (typeof data === 'string') {
            return (data.startsWith(`group by`) ? '' : 'group by ') + data
          }
        }
        return groupBy
      case 'update':
        let update = ''
        if (data) {
          if (typeof data === 'string') {
            return (data.startsWith(`update`) ? '' : 'update ') + data
          }
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
          if (typeof data === 'string') {
            return (data.startsWith(`insert`) ? '' : 'insert ') + data
          }
          for (let [key, value] of Object.entries(data)) {
            if (value === null || value === undefined) {
              delete data[key]
            }
          }
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
  async unionSelect (opts: UnionSelectOpt, transaction: any = null): Promise<any[]> {
    if (opts.if !== undefined && opts.if !== true && (opts.if as () => boolean)() !== true) {
      return []
    }
    if (!opts.from || !opts.to || !opts.on || !opts.unionType) {
      throw new ErrorHelper(`params error`)
    }
    // select
    const select = await this._assembleParam('select', opts.select)
    // on
    const on = `${opts.from}.${opts.on[0]} = ${opts.to}.${opts.on[1]}`
    // where
    const where = await this._assembleParam('where', opts.where)
    // order
    const order = await this._assembleParam('order', opts.order)
    // limit
    const limit = await this._assembleParam('limit', opts.limit)
    // forUpdate
    const forUpdate = await this._assembleParam('forUpdate', opts.forUpdate)
    // sql
    const sql = `
      select
        ${select}
      from
        ${opts.from}
      ${opts.unionType}
        ${opts.to}
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
  async update (opts: UpdateOpt, transaction: any = null): Promise<any> {
    if (opts.if !== undefined && opts.if !== true && (opts.if as () => boolean)() !== true) {
      return
    }
    // sql
    const sql = `
      update
        ${opts.from}
      set
        ${await this._assembleParam('update', opts.update)}
      ${await this._assembleParam('where', opts.where)}
    `
    const opt = {
      type: this.sequelize.QueryTypes.UPDATE,
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.info(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    return await this.query(sql, opt)
  }

  async delete (opts: DeleteOpt, transaction: any = null): Promise<any> {
    if (opts.if !== undefined && opts.if !== true && (opts.if as () => boolean)() !== true) {
      return
    }
    // sql
    const sql = `
      delete from
        ${opts.from}
      ${await this._assembleParam('where', opts.where)}
    `
    const opt = {
      type: this.sequelize.QueryTypes.DELETE,
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.info(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    return await this.query(sql, opt)
  }

  /**
   * 插入数据
   * @param opts {Object} 参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>} [id, 影响条数]
   */
  async insert (opts: InsertOpt, transaction: any = null): Promise<any> {
    if (opts.if !== undefined && opts.if !== true && (opts.if as () => boolean)() !== true) {
      return
    }
    // sql
    const sql = `
      insert into
        ${opts.from}
      ${await this._assembleParam('insert', opts.insert)}
    `
    const opt = {
      type: this.sequelize.QueryTypes.INSERT,
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.info(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    return await this.query(sql, opt)
  }

  async insertIgnore (opts: InsertOpt, transaction: any = null): Promise<any> {
    if (opts.if !== undefined && opts.if !== true && (opts.if as () => boolean)() !== true) {
      return
    }
    // sql
    const sql = `
      insert ignore into
        ${opts.from}
      ${await this._assembleParam('insert', opts.insert)}
    `
    const opt = {
      type: this.sequelize.QueryTypes.INSERT,
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.info(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    return await this.query(sql, opt)
  }

  async insertOnDuplicateKey (opts: InsertOnDuplicateKeyOpt, transaction: any = null): Promise<any> {
    if (opts.if !== undefined && opts.if !== true && (opts.if as () => boolean)() !== true) {
      return
    }
    // sql
    const sql = `
      insert into
        ${opts.from}
      ${await this._assembleParam('insert', opts.insert)}
      on duplicate key update
      ${await this._assembleParam('update', opts.update)}
    `
    const opt = {
      type: this.sequelize.QueryTypes.INSERT,
    }
    transaction && (opt['transaction'] = transaction)
    global.logger.info(`[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ''} ${sql}`)
    return await this.query(sql, opt)
  }

  /**
   * 批量插入
   * @param opts {Object} 参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>}
   */
  async batchInsert (opts: BatchInsertOpt, transaction: any = null): Promise<any> {
    if (opts.if !== undefined && opts.if !== true && (opts.if as () => boolean)() !== true) {
      return []
    }
    // sql
    const sql = `
      insert into
        ${opts.from}
      ${await this._assembleParam('batchInsert', opts.batchInsert)}
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
}

export default SequelizeHelper
