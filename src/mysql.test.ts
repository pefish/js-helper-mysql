import { Logger } from "@pefish/js-logger";
import { Mysql } from "./mysql";

describe("sequelizeHelper", () => {
  let sequelizeHelper: Mysql;

  // before(async () => {
  //   sequelizeHelper = await Mysql.new(new Logger(), {
  //     host: "localhost",
  //     username: "root",
  //     password: "root",
  //     database: "test",
  //   });
  // });

  // it("insertOnDuplicateKey", async () => {
  //   try {
  //     const results = await sequelizeHelper.insertOnDuplicateKey({
  //       from: `test`,
  //       insert: {
  //         id: `3`,
  //         mobile: `111`,
  //       },
  //       update: {
  //         mobile: `22`,
  //       },
  //     });
  //     console.error(results);
  //   } catch (err) {
  //     console.error(err);
  //     assert.throws(() => {}, err);
  //   }
  // });

  it("_assembleParam", async () => {
    return;
    const instance = new Mysql(new Logger(), null as any);
    const result = await instance._assembleParam(`order`, `order by id desc`);
    console.error(result);

    const result2 = await instance._assembleParam(`insert`, {
      a: {
        name: "Tostada cat",
        symbol: "TOSTADA",
        description: "I am Tostada , Jaiden's lovely cat. Meow.",
        image:
          "https://ipfs.io/ipfs/QmX9VEub3i829FbhaGJ82C21rY4HC72QpZCefqGgkVYZEc",
        showName: true,
        createdOn: "https://pump.fun",
        twitter: "https://x.com/JaidenAnimation",
        telegram: "https://t.me/tostadasol",
        website: "https://www.youtube.com/watch?v=Oc91iOxcB3c&t=375s",
      },
    });
    console.error("result2", result2);

    const result3 = await instance._assembleParam(`update`, {
      a: {
        name: "Tostada cat",
        symbol: "TOSTADA",
        description: "I am Tostada , Jaiden's lovely cat. Meow.",
      },
    });
    console.error("result3", result3);
  });

  it("_assembleWhere", async () => {
    return;
    const instance = new Mysql(new Logger(), null as any);
    const result1 = instance._assembleWhere({
      and: {
        source_address: `5134515`,
        status: `s:in (5,7)`,
      },
    });
    console.error("result1", result1);

    const result2 = instance._assembleWhere({
      or: {
        source_address: `5134515`,
        status: `s:in (5,7)`,
        chain: `fadf`,
      },
    });
    console.error("result2", result2);

    const result3 = await instance._assembleWhere({
      and: {
        source_address: `5134515`,
        status: [5, 7],
        chain: `fadf`,
        c: 4,
      },
      or: {
        a: "a",
        b: 4,
      },
    });
    console.error("result3", result3);

    const result4 = await instance._assembleWhere(
      `
a = "a"
and
b = 3
`
    );
    console.error("result4", result4);

    const result5 = await instance._assembleWhere({
      and: `a=2 and b=3`,
      or: {
        c: 5,
        d: 6,
      },
    });
    console.error("result5", result5);
  });

  // it("select", async () => {
  //   try {
  //     const results = await sequelizeHelper.select({
  //       select: `*`,
  //       from: `test`,
  //       where: {
  //         mobile: `11`,
  //       },
  //     });
  //     console.error(results);
  //   } catch (err) {
  //     console.error(err);
  //     assert.throws(() => {}, err);
  //   }
  // });

  // it("sum", async () => {
  //   try {
  //     const results = await sequelizeHelper.sum({
  //       sum: `id`,
  //       from: `test`,
  //       where: {
  //         mobile: `32452`,
  //       },
  //     });
  //     console.error(results);
  //   } catch (err) {
  //     console.error(err);
  //     assert.throws(() => {}, err);
  //   }
  // });

  // it("selectBySql", async () => {
  //   try {
  //     const tran = await sequelizeHelper.begin();
  //     const results = await sequelizeHelper.selectBySql(
  //       "select * from test",
  //       {},
  //       tran
  //     );
  //     console.error(results);
  //     await sequelizeHelper.commit(tran);
  //   } catch (err) {
  //     console.error(err);
  //     assert.throws(() => {}, err);
  //   }
  // });

  // after(() => {
  //   sequelizeHelper.close();
  // });

  // it("createDatabase", async () => {
  //   try {
  //     await sequelizeHelper.createDatabase("test1", true);
  //     await sequelizeHelper.query("use test;");
  //     // logger.error(result)
  //   } catch (err) {
  //     console.error(err);
  //     assert.throws(() => {}, err);
  //   } finally {
  //     await sequelizeHelper.dropDatabase("test1");
  //   }
  // });

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

  // it("insert", async () => {
  //   try {
  //     const result = await sequelizeHelper.insert({
  //       insert: {
  //         mobile: `haha`,
  //       },
  //       from: "test",
  //     });
  //     console.error(result);
  //     // assert.strictEqual(result.length, 2)
  //   } catch (err) {
  //     console.error(err);
  //     assert.throws(() => {}, err);
  //   }
  // });

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
});
