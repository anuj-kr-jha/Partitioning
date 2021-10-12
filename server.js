const { Client, Pool } = require('pg');
const { nanoid } = require('nanoid');
const express = require('express');
const app = express();
app.use(express.json());

const db = 'test';
const table = 'todo';
const config = { user: 'postgres', password: 'postgres', host: 'localhost', port: 5432 };
const client = new Client({ ...config });
const pool = new Pool({ ...config, db });

app.listen(8080, () => console.log('Web server is listening.. on port 8080'));

main();

async function main() {
    try {
        await connect();
        test();
    } catch {
        console.log('error');
    }
}

async function test() {
    // await describeTable();
    await createTodo(1000);
    // const data = await readAllTodos(table);
    // console.log(data);
}

async function connect() {
    try {
        await client.connect();
        // console.log(`db connected`);
        // console.log(`dropping db ${db}`);
        // await dropDB(db);
        // console.log(`creating db ${db}`);
        // await createDB(db);
        // console.log(`creating table schema ${table} with partition`);
        // await createTableWithPartition(table);
    } catch (e) {
        console.error(`Failed to connect ${e}`);
    } finally {
        await client.end();
    }
}

const createDB = async (db) => {
    try {
        const dbs = await client.query(`CREATE DATABASE ${db}`);
    } catch (error) {
        console.log({ error });
    }
};

const dropDB = async (db) => {
    try {
        const dbs = await client.query(`DROP DATABASE ${db}`);
    } catch (error) {
        console.log('dropping non existing db');
    }
};

const createTableWithPartition = async (table) => {
    try {
        await pool.query(`
            CREATE TABLE ${table} (
                id integer NOT NULL CONSTRAINT todo_pk PRIMARY KEY,
                s_task_name text,
                n_assigned_hour integer
            )
            partition by range (id)
        `);
        console.log('partition creation success');
    } catch (e) {
        console.log('error: table schema already exist fot table', table);
        console.log(`dropping table : ${table}`);
        await dropTable(table);
        await pool.query(`
            CREATE TABLE ${table} (
                id integer NOT NULL CONSTRAINT todo_pk PRIMARY KEY,
                s_task_name text,
                n_assigned_hour integer
            )
            partition by range (id)
        `);
        console.log('partition creation success');
    } finally {
        let k = 0;
        for (let p = 1; p <= 10; p++) {
            const idBgn = 1 + k;
            const idEnd = 1000 + k;
            const partitionName = `todo_${idBgn}_${idEnd}`;
            // await pool.query(`drop table ${partitionName}`);
            k += 999;
            const partitionCreationQuery = `
                create table ${partitionName}
                (like ${table} including indexes)
            
            `;
            const partitionAttachmentQuery = `
                alter table ${table}
                attach partition ${partitionName}
                for values from (${idBgn}) to (${idEnd})
            `;
            await pool.query(partitionCreationQuery);
            await pool.query(partitionAttachmentQuery);
            console.log('partitition created: ', partitionName);
        }
    }
};

const dropTable = async (table) => {
    await client.query(`DROP TABLE ${table}`);
};

const describeTable = async () => {
    const query = {
        name: 'describe-tables',
        text: `
            SELECT 
                table_name, 
                column_name,
                data_type
            FROM
                information_schema.columns
            WHERE
                table_name = 'todo';
        `,
    };
    const { rows: schema } = await pool.query(query);
    console.table(schema);
};

async function createTodo(num) {
    try {
        const prevData = (await readAllTodos(table)).length;
        for (i = prevData + 1; i < prevData + num; i++) {
            const query = {
                name: 'create-todos',
                text: 'INSERT INTO todo(id, n_assigned_hour, s_task_name) VALUES($1, $2, $3)',
                values: [i, i % 10, nanoid(6)],
            };
            await pool.query(query);
        }
        console.log(`${num} todo creation success`);
    } catch (e) {
        console.log('todo creation failed', e);
    }
}

async function readAllTodos(table) {
    const query = {
        name: 'read-todos',
        text: `SELECT * FROM ${table}`,
    };
    try {
        const results = await pool.query(query);
        return results.rows;
    } catch (e) {
        return [];
    }
}

async function readOneTodos(id, table) {
    const query = {
        name: 'read-todos',
        text: `SELECT * FROM ${table} WHERE id = $1;`,
        values: [id],
    };
    try {
        const results = await pool.query(query);
        return results.rows;
    } catch (e) {
        return [];
    }
}
