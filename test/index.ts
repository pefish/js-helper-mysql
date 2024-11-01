import Starter from "@pefish/js-util-starter";
import SequelizeHelper from "../src/mysql";

Starter.startAsync(async () => {
  const sequelizeHelper = new SequelizeHelper({
    host: "localhost",
    username: "root",
    password: "root",
    database: "test",
  });
  await sequelizeHelper.init();

  await sequelizeHelper.close();
  console.log(`11`);
});
