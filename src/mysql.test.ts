import '@pefish/js-node-assist'
import * as assert from 'assert'
import SequelizeHelper from './mysql'

describe('sequelizeHelper', () => {

  let sequelizeHelper

  before(async () => {
    sequelizeHelper = new SequelizeHelper({
      'host': 'localhost',
      'username': 'root',
      'password': 'root',
      'database': 'test'
    })
    await sequelizeHelper.init()
    // await sequelizeHelper.createDatabase('test', true)
  })

  it('select', async () => {
    try {
      const results = await sequelizeHelper.select({
        select: `*`,
        from: `test`,
        where: {
          user_id: undefined,
        }
      })
      global.logger.error(results)
    } catch (err) {
      global.logger.error(err)
      assert.throws(() => {}, err)
    }
  })

  it('selectBySql', async () => {
    try {
      const tran = await sequelizeHelper.begin()
      const results = await sequelizeHelper.selectBySql('select * from test', {}, tran)
      await sequelizeHelper.commit(tran)
      global.logger.error(results)
    } catch (err) {
      global.logger.error(err)
      assert.throws(() => {}, err)
    }
  })

  after(async () => {
    // await sequelizeHelper.dropDatabase('test')
    await sequelizeHelper.close()
  })

  // it('createDatabase', async () => {
  //   try {
  //     await sequelizeHelper.createDatabase('test1', true)
  //     await sequelizeHelper.query('use test;')
  //     // logger.error(result)
  //   } catch (err) {
  //     logger.error(err)
  //     assert.throws(() => {}, err)
  //   } finally {
  //     await sequelizeHelper.dropDatabase('test1')
  //   }
  // })
  //
  // it('createBySql创建数据库', async () => {
  //   try {
  //     const bool = await sequelizeHelper.createBySql(`
  //       create database test2
  //     `)
  //     await sequelizeHelper.query('use test2;')
  //     // logger.error(result)
  //     assert.strictEqual(bool, true)
  //   } catch (err) {
  //     logger.error(err)
  //     assert.throws(() => {}, err)
  //   } finally {
  //     await sequelizeHelper.dropDatabase('test2')
  //   }
  // })
  //
  // it('createBySql创建表', async () => {
  //   try {
  //     await sequelizeHelper.query('use test;')
  //     const bool = await sequelizeHelper.createBySql(`
  //       CREATE TABLE cold_wallet (
  //         id int(11) unsigned NOT NULL AUTO_INCREMENT,
  //         currency_id int(11) NOT NULL COMMENT '货币id',
  //         address varchar(100) NOT NULL COMMENT '地址',
  //         created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  //         updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  //         PRIMARY KEY (id)
  //       ) ENGINE=InnoDB AUTO_INCREMENT=108 DEFAULT CHARSET=utf8 COMMENT='冷钱包';
  //     `)
  //     // logger.error(result)
  //
  //     assert.strictEqual(bool, true)
  //   } catch (err) {
  //     logger.error(err)
  //     assert.throws(() => {}, err)
  //   }
  // })
  //
  // it('executeSqlFile', async () => {
  //   try {
  //     await sequelizeHelper.query('use test;')
  //     await sequelizeHelper.executeSqlFile(path.join(FileUtil.getWorkPath(), 'tests/fixtures/sequelizeHelper.sql'))
  //     // logger.error(result)
  //     // assert.strictEqual(result.length, 2)
  //   } catch (err) {
  //     logger.error(err)
  //     assert.throws(() => {}, err)
  //   }
  // })

  it('insert', async () => {
    try {
      const result = await sequelizeHelper.insert(
        {
          insert: {
            user_id: 56,
            name: `haha`,
          },
          from: 'test'
        }
      )
      global.logger.error(result)
      // assert.strictEqual(result.length, 2)
    } catch (err) {
      global.logger.error(err)
      assert.throws(() => {}, err)
    }
  })

  // it('delete', async () => {
  //   try {
  //     await sequelizeHelper.query('use test;')
  //     const result = await sequelizeHelper.insert(
  //       {
  //         insert: {
  //           currency_id: 9,
  //           address: 'hertyethetryw'
  //         },
  //         from: 'cold_wallet'
  //       }
  //     )
  //     assert.strictEqual(result[0], 111)
  //     const a = await sequelizeHelper.delete({
  //       from: 'cold_wallet',
  //       where: {
  //         id: result[0]
  //       }
  //     })
  //     // logger.error(result, a)
  //     // assert.strictEqual(result.length, 2)
  //   } catch (err) {
  //     logger.error(err)
  //     assert.throws(() => {}, err)
  //   }
  // })
})
