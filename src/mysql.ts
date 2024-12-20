import { DataType, getType, ILogger } from "@pefish/js-logger";
import fs from "fs";
import { QueryOptions } from "mysql2";
import { PoolOptions, QueryTypes, Sequelize, Transaction } from "sequelize";

export interface MysqlConfigration {
  host: string;
  port?: number;
  username: string;
  password: string;
  database: string;
  pool?: PoolOptions;
}

interface SelectOpt {
  select: string | string[];
  from: string;
  where?: WhereDataType | string | null | undefined;
  order?: string | [string, string][];
  limit?: string | number[];
  groupBy?: string | string[];
  forUpdate?: boolean;
  if?: boolean | (() => boolean);
}

interface SumOpt {
  sum: string;
  from: string;
  where?: WhereDataType | string | null | undefined;
  if?: boolean | (() => boolean);
}

interface CountOpt {
  from: string;
  where: WhereDataType | string | null | undefined;
  if?: boolean | (() => boolean);
}

interface UnionSelectOpt {
  from: string;
  to: string;
  unionType: string;
  where?: WhereDataType | string | null | undefined;
  order?: string | [string, string][];
  limit?: string | number[];
  select: string | string[];
  on: string[];
  forUpdate?: boolean;
  if?: boolean | (() => boolean);
}

interface UpdateOpt {
  update:
    | string
    | {
        [x: string]: any;
      };
  from: string;
  where: WhereDataType | string | null | undefined;
  if?: boolean | (() => boolean);
}

interface DeleteOpt {
  from: string;
  where: WhereDataType | string | null | undefined;
  if?: boolean | (() => boolean);
}

interface InsertOpt<T> {
  insert: T;
  from: string;
  if?: boolean | (() => boolean);
}

interface InsertOnDuplicateKeyOpt<T> {
  insert: T;
  update: {
    [x: string]: any;
  };
  from: string;
  if?: boolean | (() => boolean);
}

interface BatchInsertOpt {
  batchInsert: [string[], string[][]];
  from: string;
  if?: boolean | (() => boolean);
}

type WhereObjectType = { [x: string]: string | number | (string | number)[] };

interface WhereDataType {
  and?: WhereObjectType | string | null | undefined; // 表示 WhereObjectType 中全是 and 的关系
  or?: WhereObjectType | string | null | undefined;
}

export class Mysql {
  public sequelize: Sequelize;
  private logger: ILogger;

  constructor(logger: ILogger, sequelize: Sequelize) {
    this.logger = logger;
    this.sequelize = sequelize;
  }

  static async new(
    logger: ILogger,
    config: MysqlConfigration,
    dbType: string = "mysql"
  ): Promise<Mysql> {
    let sequelize: Sequelize;
    if (dbType === "mysql") {
      sequelize = new Sequelize(
        config.database,
        config.username,
        config.password,
        {
          host: config.host,
          port: config.port || 3306,
          dialect: "mysql",
          pool: {
            max: 30,
            min: 5,
            idle: 10000,
            acquire: 100000,
            ...config.pool,
          },
          define: {
            timestamps: true,
            underscored: true,
            paranoid: false,
            freezeTableName: true,
          },
          logging: (sql) => {
            // global[`debug`] && logger.info(sql)
          },
          dialectOptions: {
            multipleStatements: true,
            // useUTC: true  // 设置读到的时间的时区
          },
          timezone: "+00:00", // 设置写入的时间的时区
        }
      );
    } else if (dbType === "sqlite") {
      sequelize = new Sequelize(config.database, "", "", {
        dialect: "sqlite",
        storage: config.host,
        logging: (sql) => {
          // global[`debug`] && logger.info(sql)
        },
      });
    } else {
      throw new Error(`dbType 有误。dbType: ${dbType}`);
    }

    const mysqlInstance = new Mysql(logger, sequelize);
    mysqlInstance.logger.info(`connecting: ${config.host} ...`);
    await mysqlInstance.sequelize.authenticate();
    mysqlInstance.logger.info(`connection succeeded: ${config.host}`);
    return mysqlInstance;
  }

  async query(sql: string, opt: QueryOptions | null = null): Promise<any> {
    return opt
      ? this.sequelize.query(sql, opt as any)
      : this.sequelize.query(sql);
  }

  regularString(str: any): string {
    let result: string;
    switch (Object.prototype.toString.call(str)) {
      case "[object Object]":
        result = JSON.stringify(str);
        break;
      case "[object Array]":
        result = JSON.stringify(str);
        break;
      case "[object Null]":
        result = "NULL";
        break;
      case "[object Undefined]":
        result = "NULL";
        break;
      default:
        result = str.toString();
        break;
    }
    return result.replace(/\\/g, "\\\\").replace(/\'/g, "\\'");
  }

  /**
   * 执行脚本文件
   * @param filename {string} 绝对路径
   * @returns {Promise<void>}
   */
  async executeSqlFile<T>(filename: string): Promise<T> {
    const sql = `BEGIN;${fs.readFileSync(filename).toString()}COMMIT;`;
    this.logger.debug(`[sql] ${sql}`);
    return await this.sequelize.query(sql, {
      raw: true,
    });
  }

  async executeSql<T>(sql: string): Promise<T> {
    const sql1 = `BEGIN;${sql}COMMIT;`;
    this.logger.debug(`[sql] ${sql1}`);
    return await this.sequelize.query(sql1, {
      raw: true,
    });
  }

  /**
   * 开启事务
   * @returns {Promise<*|Transaction>}
   */
  async begin(): Promise<Transaction> {
    const transaction = await this.sequelize.transaction();
    this.logger.debug(`[sql] [transactionId: ${transaction.id}] begin`);
    await transaction.prepareEnvironment();
    return transaction;
  }

  /**
   * 提交事务。支持多次调用
   * @param transaction {Transaction} 事务实例
   * @returns {Promise<Promise<*>|Promise|*|Object>}
   */
  async commit(transaction: any): Promise<any> {
    if (!transaction.finished) {
      this.logger.debug(`[sql] [transactionId: ${transaction.id}] commit`);
      return await transaction.commit();
    }
  }

  /**
   * 回滚事务。支持多次调用
   * @param transaction {Transaction} 事务实例
   * @returns {Promise<T>}
   */
  async rollback(transaction: any): Promise<any> {
    if (!transaction.finished) {
      this.logger.debug(`[sql] [transactionId: ${transaction.id}] rollback`);
      return await transaction.rollback().catch(() => {});
    }
  }

  /**
   * 使用select sql
   * @param sql {String} sql
   * @param replacements {Object} 替换参数, default: {}
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>}
   */
  async selectBySql<T>(
    sql: string,
    replacements: object = {},
    transaction: any = null
  ): Promise<T[]> {
    const opt = {
      type: QueryTypes.SELECT,
      replacements: replacements,
      transaction,
    };
    this.logger.debug(
      `[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ""} ${sql}`
    );
    const results = await this.query(sql, opt as any);
    if (!results || results.length <= 0) {
      return [];
    }
    return results;
  }

  /**
   * 使用create sql,创建数据库、表
   * @param sql {String} sql
   * @param replacements {Object} 替换参数, default: {}
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<boolean>}
   */
  async createBySql(
    sql: string,
    replacements: object = {},
    transaction: any = null
  ): Promise<boolean> {
    const opt = {
      type: QueryTypes.UPDATE,
      replacements: replacements,
      transaction,
    };
    this.logger.debug(
      `[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ""} ${sql}`
    );
    await this.query(sql, opt as any);
    return true;
  }

  /**
   * 创建数据库
   * @param databaseName
   * @param force 存在就drop掉
   * @returns {Promise<*>}
   */
  async createDatabase(
    databaseName: string,
    force: boolean = false
  ): Promise<any> {
    try {
      force === true && (await this.dropDatabase(databaseName));
    } catch (err) {}
    const sql = `create database ${databaseName}`;
    this.logger.debug(`[sql] ${sql}`);
    return await this.query(sql);
  }

  async dropDatabase(databaseName: string): Promise<any> {
    const sql = `drop database ${databaseName}`;
    this.logger.debug(`[sql] ${sql}`);
    return await this.query(sql);
  }

  /**
   * select查询
   * @param opts {Object} 查询参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>}
   */
  async select<T>(opts: SelectOpt, transaction: any = null): Promise<T[]> {
    if (
      opts.if !== undefined &&
      opts.if !== true &&
      (opts.if as () => boolean)() !== true
    ) {
      return [];
    }
    // sql
    const sql = `
      select
        ${opts.select}
      from
        ${opts.from}
      ${this._assembleWhere(opts.where)}
      ${await this._assembleParam("order", opts.order)}
      ${await this._assembleParam("limit", opts.limit)}
      ${await this._assembleParam("groupBy", opts.groupBy)}
      ${await this._assembleParam("forUpdate", opts.forUpdate)}
    `;
    const opt = {
      type: QueryTypes.SELECT,
      transaction,
    };
    this.logger.debug(
      `[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ""} ${sql}`
    );
    const results = await this.query(sql, opt as any);
    if (!results || results.length <= 0) {
      return [];
    }
    return results;
  }

  /**
   * select查询
   * @param opts {Object} 查询参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>}
   */
  async selectOne<T>(
    opts: SelectOpt,
    transaction: any = null
  ): Promise<T | null> {
    const results = await this.select<T>(opts, transaction);
    if (results.length === 0) {
      return null;
    }
    return results[0];
  }

  /**
   * 求和
   * @param opts {Object} 参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>}
   */
  async sum(opts: SumOpt, transaction: any = null): Promise<string> {
    if (
      opts.if !== undefined &&
      opts.if !== true &&
      (opts.if as () => boolean)() !== true
    ) {
      return `0`;
    }
    // sql
    const sql = `
      select
        sum(${opts.sum}) as sum
      from
        ${opts.from}
      ${this._assembleWhere(opts.where)}
    `;
    const opt = {
      type: QueryTypes.SELECT,
      transaction,
    };
    this.logger.debug(
      `[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ""} ${sql}`
    );
    const results = await this.query(sql, opt as any);
    if (!results || results.length <= 0 || !results[0]["sum"]) {
      return `0`;
    }
    return results[0]["sum"].toString() || `0`;
  }

  /**
   * 关闭数据库连接
   * @returns {Promise<void>}
   */
  async close(): Promise<any> {
    return this.sequelize && (await this.sequelize.close());
  }

  /**
   * 求总数
   * @param opts {Object} 参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>}
   */
  async count(opts: CountOpt, transaction: any = null): Promise<number> {
    if (
      opts.if !== undefined &&
      opts.if !== true &&
      (opts.if as () => boolean)() !== true
    ) {
      return 0;
    }
    // sql
    const sql = `
      select
        count(*) as count
      from
        ${opts.from}
      ${this._assembleWhere(opts.where)}
    `;
    const opt = {
      type: QueryTypes.SELECT,
      transaction,
    };
    this.logger.debug(
      `[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ""} ${sql}`
    );
    const results = await this.query(sql, opt as any);
    if (!results || results.length <= 0) {
      return 0;
    }
    return results[0]["count"];
  }

  _assembleWhere(
    whereData_: string | WhereDataType | null | undefined
  ): string {
    if (!whereData_) {
      return "";
    }
    const whereDataType: DataType = getType(whereData_);
    if (whereDataType === "String") {
      return (whereData_ as string).startsWith("where ")
        ? (whereData_ as string)
        : `where ${whereData_}`;
    }

    const _assembleWhereObject = (objectValue: Object): string[] => {
      const arr: string[] = [];
      for (let [field, value] of Object.entries(objectValue)) {
        const valueType = getType(value);
        if (valueType === "String" && (value as string).startsWith("s:")) {
          arr.push(`${field} ${(value as string).substring(2).trim()}`);
        } else if (valueType === "Array") {
          const inStr = `'${(value as any[])
            .map((vEle: any) => {
              return this.regularString(vEle);
            })
            .join("','")}'`;
          arr.push(`${field} in (${inStr})`);
        } else {
          arr.push(`${field} = '${this.regularString(value)}'`);
        }
      }
      return arr;
    };

    let andWhereStr = "";
    const whereData = whereData_ as WhereDataType;
    if (whereData.and) {
      const andValue = whereData.and;
      const andValueType: DataType = getType(andValue);
      if (andValueType === "String") {
        andWhereStr = andValue as string;
      } else if (andValueType === "Object") {
        andWhereStr = _assembleWhereObject(andValue as Object).join(" and ");
      }
    }

    let orWhereStr = "";
    if (whereData.or) {
      const orValue = whereData.or;
      const orValueType: DataType = getType(orValue);
      if (orValueType === "String") {
        orWhereStr = orValue as string;
      } else if (orValueType === "Object") {
        orWhereStr = _assembleWhereObject(orValue as Object).join(" or ");
      }
    }

    if (!andWhereStr && !orWhereStr) {
      return "";
    }

    if (andWhereStr && orWhereStr) {
      return `where ${andWhereStr} and (${orWhereStr})`;
    }

    return `where ${andWhereStr || orWhereStr}`;
  }

  async _assembleParam(name: string, data: any): Promise<string> {
    switch (name) {
      case "select":
        let select = "";
        if (typeof data === "string") {
          return (data.startsWith(`select`) ? "" : "select ") + data;
        }
        if (!data || data.length <= 0) {
          select = "*";
        } else {
          select = data
            .map((val: any) => {
              return val;
            })
            .join(",");
        }
        return select;
      case "order":
        let order = "";
        if (data) {
          if (typeof data === "string") {
            return (data.startsWith(`order by`) ? "" : "order by ") + data;
          }
          order =
            order +
            "order by " +
            data
              .map((val: any) => {
                return `${val[0]} ${val[1]}`;
              })
              .join(",");
        }
        return order;
      case "limit":
        let limit = "";
        if (data) {
          if (typeof data === "string") {
            return (data.startsWith(`limit`) ? "" : "limit ") + data;
          }
          limit = `limit ${data[0]}, ${data[1]}`;
        }
        return limit;
      case "forUpdate":
        let forUpdate = "";
        if (data) {
          if (typeof data === "string") {
            return (data.startsWith(`for update`) ? "" : "for update ") + data;
          }
          if (data === true) {
            forUpdate = "for update";
          }
        }
        return forUpdate;
      case "groupBy":
        const groupBy = "";
        if (data) {
          if (typeof data === "string") {
            return (data.startsWith(`group by`) ? "" : "group by ") + data;
          }
        }
        return groupBy;
      case "update":
        let update = "";
        if (data) {
          if (typeof data === "string") {
            return (data.startsWith(`update`) ? "" : "update ") + data;
          }
          for (const [key, value] of Object.entries(data)) {
            if (value !== undefined && value !== null) {
              update =
                update +
                `${key} = '${this.regularString(value as string | number)}', `;
            }
          }
          update.length > 0 &&
            (update = update.substring(0, update.length - 2) + " ");
        }
        return update;
      case "insert":
        let insert = "";
        if (data) {
          if (typeof data === "string") {
            return (data.startsWith(`insert`) ? "" : "insert ") + data;
          }
          for (let [key, value] of Object.entries(data)) {
            if (value === null || value === undefined) {
              delete data[key];
            }
          }
          const fields = Object.keys(data).join(","),
            values = Object.values(data)
              .map((val: any) => `'${this.regularString(val)}'`)
              .join(",");
          insert = `(${fields}) values (${values})`;
        }
        return insert;
      case "batchInsert":
        let batchInsert = "";
        if (data) {
          const fields = data[0].join(",");
          const values = data[1]
            .map((val: any) => {
              return `(${val
                .map((a: any) => {
                  if (!a) {
                    return "null";
                  }
                  return `'${this.regularString(a)}'`;
                })
                .join(",")})`;
            })
            .join(",");
          batchInsert = `(${fields}) values ${values}`;
        }
        return batchInsert;
      default:
        return "";
    }
  }

  /**
   * 联表查询
   * @param opts {Object} 参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>}
   */
  async unionSelect<T>(
    opts: UnionSelectOpt,
    transaction: any = null
  ): Promise<T[]> {
    if (
      opts.if !== undefined &&
      opts.if !== true &&
      (opts.if as () => boolean)() !== true
    ) {
      return [];
    }
    if (!opts.from || !opts.to || !opts.on || !opts.unionType) {
      throw new Error(`params error`);
    }
    // select
    const select = await this._assembleParam("select", opts.select);
    // on
    const on = `${opts.from}.${opts.on[0]} = ${opts.to}.${opts.on[1]}`;
    // where
    const where = this._assembleWhere(opts.where);
    // order
    const order = await this._assembleParam("order", opts.order);
    // limit
    const limit = await this._assembleParam("limit", opts.limit);
    // forUpdate
    const forUpdate = await this._assembleParam("forUpdate", opts.forUpdate);
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
    `;
    const opt = {
      type: QueryTypes.SELECT,
      transaction,
    };
    this.logger.debug(
      `[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ""} ${sql}`
    );
    const results = await this.query(sql, opt as any);
    if (!results || results.length <= 0) {
      return [];
    }
    return results;
  }

  /**
   * update操作
   * @param opts {Object} 参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>}
   */
  async update(opts: UpdateOpt, transaction: any = null): Promise<any> {
    if (
      opts.if !== undefined &&
      opts.if !== true &&
      (opts.if as () => boolean)() !== true
    ) {
      return;
    }
    // sql
    const sql = `
      update
        ${opts.from}
      set
        ${await this._assembleParam("update", opts.update)}
      ${this._assembleWhere(opts.where)}
    `;
    const opt = {
      type: QueryTypes.UPDATE,
      transaction,
    };
    this.logger.debug(
      `[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ""} ${sql}`
    );
    return await this.query(sql, opt as any);
  }

  async delete(opts: DeleteOpt, transaction: any = null): Promise<any> {
    if (
      opts.if !== undefined &&
      opts.if !== true &&
      (opts.if as () => boolean)() !== true
    ) {
      return;
    }
    // sql
    const sql = `
      delete from
        ${opts.from}
      ${this._assembleWhere(opts.where)}
    `;
    const opt = {
      type: QueryTypes.DELETE,
      transaction,
    };
    this.logger.debug(
      `[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ""} ${sql}`
    );
    return await this.query(sql, opt as any);
  }

  /**
   * 插入数据
   * @param opts {Object} 参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>} [id, 影响条数]
   */
  async insert<T>(opts: InsertOpt<T>, transaction: any = null): Promise<any> {
    if (
      opts.if !== undefined &&
      opts.if !== true &&
      (opts.if as () => boolean)() !== true
    ) {
      return;
    }
    // sql
    const sql = `
      insert into
        ${opts.from}
      ${await this._assembleParam("insert", opts.insert)}
    `;
    const opt = {
      type: QueryTypes.INSERT,
      transaction,
    };
    this.logger.debug(
      `[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ""} ${sql}`
    );
    return await this.query(sql, opt as any);
  }

  async insertIgnore<T>(
    opts: InsertOpt<T>,
    transaction: any = null
  ): Promise<any> {
    if (
      opts.if !== undefined &&
      opts.if !== true &&
      (opts.if as () => boolean)() !== true
    ) {
      return;
    }
    // sql
    const sql = `
      insert ignore into
        ${opts.from}
      ${await this._assembleParam("insert", opts.insert)}
    `;
    const opt = {
      type: QueryTypes.INSERT,
      transaction,
    };
    this.logger.debug(
      `[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ""} ${sql}`
    );
    return await this.query(sql, opt as any);
  }

  async insertOnDuplicateKey<T>(
    opts: InsertOnDuplicateKeyOpt<T>,
    transaction: any = null
  ): Promise<any> {
    if (
      opts.if !== undefined &&
      opts.if !== true &&
      (opts.if as () => boolean)() !== true
    ) {
      return;
    }
    // sql
    const sql = `
      insert into
        ${opts.from}
      ${await this._assembleParam("insert", opts.insert)}
      on duplicate key update
      ${await this._assembleParam("update", opts.update)}
    `;
    const opt = {
      type: QueryTypes.INSERT,
      transaction,
    };
    this.logger.debug(
      `[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ""} ${sql}`
    );
    return await this.query(sql, opt as any);
  }

  /**
   * 批量插入
   * @param opts {Object} 参数
   * @param transaction {Transaction} 事务实例, default: null
   * @returns {Promise<*>}
   */
  async batchInsert(
    opts: BatchInsertOpt,
    transaction: any = null
  ): Promise<any> {
    if (
      opts.if !== undefined &&
      opts.if !== true &&
      (opts.if as () => boolean)() !== true
    ) {
      return [];
    }
    // sql
    const sql = `
      insert into
        ${opts.from}
      ${await this._assembleParam("batchInsert", opts.batchInsert)}
    `;
    const opt = {
      type: QueryTypes.INSERT,
      transaction,
    };
    this.logger.debug(
      `[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ""} ${sql}`
    );
    return await this.query(sql, opt as any);
  }

  async startTransaction(fun: (tran: any) => Promise<void>): Promise<void> {
    const tran = await this.begin();
    try {
      await fun(tran);
      await this.commit(tran);
    } catch (err) {
      await this.rollback(tran);
      throw err;
    }
  }

  /**
   * 使用update sql
   * @param sql {String} sql
   * @param replacements {Object} 替换参数, default: {}
   * @param transaction {Transaction} 事务实例
   * @returns {Promise<*>}
   */
  async updateBySql(
    sql: string,
    replacements: object = {},
    transaction: any = null
  ): Promise<any> {
    const opt = {
      type: QueryTypes.UPDATE,
      replacements: replacements,
      transaction,
    };
    this.logger.debug(
      `[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ""} ${sql}`
    );
    return await this.query(sql, opt as any);
  }

  async deleteBySql(
    sql: string,
    replacements: object = {},
    transaction: any = null
  ): Promise<any> {
    const opt = {
      type: QueryTypes.DELETE,
      replacements: replacements,
      transaction,
    };
    this.logger.debug(
      `[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ""} ${sql}`
    );
    return await this.query(sql, opt as any);
  }

  /**
   * 使用insert sql
   * @param sql {String} sql
   * @param replacements {Object} 替换参数, default: {}
   * @param transaction {Transaction} 事务实例
   * @returns {Promise<*>}
   */
  async insertBySql(
    sql: string,
    replacements: object = {},
    transaction: any = null
  ): Promise<any> {
    const opt = {
      type: QueryTypes.INSERT,
      replacements: replacements,
      transaction,
    };
    this.logger.debug(
      `[sql] ${transaction ? `[transactionId: ${transaction.id}]` : ""} ${sql}`
    );
    return await this.query(sql, opt as any);
  }
}
