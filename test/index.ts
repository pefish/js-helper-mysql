import Starter from "@pefish/js-util-starter";
import SequelizeHelper from "../src/mysql";

Starter.startAsync(async () => {
  const sequelizeHelper = new SequelizeHelper({
    host: "",
    username: "pefish_me",
    password: "",
    database: "pefish_me",
  });
  await sequelizeHelper.init();

  await sequelizeHelper.insert({
    insert: {
      address: "",
      token_address: "111",
      init_amount: "111",
      init_token_amount: "111",
      current_token_amount: "111",
      init_timestamp: 1132231,
      records: JSON.stringify([
        {
          aaa: "dvsdf",
        },
      ]),
    },
    from: "sol_pos",
  });

  await sequelizeHelper.close();
  console.log(`11`);
});
