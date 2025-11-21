// nodejs-server.js
import cron from 'node-cron';
import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import mysql from 'mysql2/promise';
import dotenv from 'dotenv';
import cors from 'cors';
import fetch from 'node-fetch';
import zlib from 'zlib';
import fs from 'fs';

dotenv.config();

/* ========== Pools ========== */
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: process.env.DB_PORT
});

const cronDB = mysql.createPool({
  host: process.env.CRON_DB_HOST,
  user: process.env.CRON_DB_USER,
  password: process.env.CRON_DB_PASSWORD,
  database: process.env.CRON_DB_NAME,
  port: process.env.CRON_DB_PORT
});

/* ========== Utility functions ========== */
async function tableExists(queryName) {
  const [rows] = await pool.query(`SHOW TABLES LIKE ?`, [queryName]);
  return rows.length > 0;
}

const phpApiUrl = process.env.PHP_API_URL;
async function fetchSqlTemplate(hisType, queryName) {
  const url = `${phpApiUrl}?hisType=${encodeURIComponent(hisType)}&queryName=${encodeURIComponent(queryName)}`;
  const res = await fetch(url, { timeout: 20000 });
  if (!res.ok) throw new Error(`PHP API error: ${res.status} ${res.statusText}`);
  const data = await res.json();
  if (!data || !data.sql) throw new Error(`ไม่พบ SQL template จาก ${url}`);
  return data.sql;
}

/* ========== App/Socket setup ========== */
const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 512 * 1024 * 1024 });

// อ่าน ALLOWED_ORIGINS จาก .env (comma-separated)
const allowedOrigins = process.env.ALLOWED_ORIGINS
  ? process.env.ALLOWED_ORIGINS.split(',').map(origin => origin.trim())
  : [];

app.use(cors({
  origin: function (origin, callback) {
    if (!origin || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error('⛔ Origin ไม่ได้รับอนุญาต'));
    }
  }
}));

let clients = {};
const chunkBuffers = {};

/* ========== Ensure table (สร้างครั้งแรกเท่านั้น) ========== */
async function ensureTable(queryName, sampleRow) {
  const [rows] = await pool.query(`SHOW TABLES LIKE ?`, [queryName]);
  if (rows.length > 0) return; // มีแล้ว

  const uniqueKeys = [...new Set(Object.keys(sampleRow))];
  const columnsDef = uniqueKeys.map(k => `\`${k}\` TEXT`).join(', ');

  const createSQL = `
    CREATE TABLE \`${queryName}\` (
      id INT AUTO_INCREMENT PRIMARY KEY,
      ${columnsDef}
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
  `;
  await pool.query(createSQL);
  console.log(`🧱 Created table ${queryName}`);
}

/* ========== Socket events ========== */
io.on('connection', (socket) => {
  socket.on('register', (clientInfo) => {
    const { hosCode, hosName } = clientInfo;
    clients[hosCode] = socket;
    console.log(`✅ REGISTER hosCode=${hosCode}, hosName=${hosName}, socket=${socket.id}`);
    socket.emit('greeting', `<-- Server: ยินดีต้อนรับ [${hosCode}]`);
  });

  socket.on('clientMetric', (metric) => {
    console.log('📊 clientMetric:', metric);
  });

  socket.on('clientData', async (data) => {
    const { hosCode, queryName, data: payload, compressed, chunkIndex, chunkTotal } = data;

    if (!clients[hosCode]) {
      console.warn(`⚠️ clientData ก่อน register: ${hosCode}`);
      socket.emit('error', 'Client not registered');
      return;
    }

    try {
      let jsonStr;
      if (compressed) {
        const buf = Buffer.from(payload, 'base64');
        jsonStr = zlib.gunzipSync(buf).toString('utf8');
      } else {
        jsonStr = payload;
      }
      const rows = JSON.parse(jsonStr);

      const key = `${hosCode}_${queryName}`;
      if (!chunkBuffers[key]) chunkBuffers[key] = [];
      chunkBuffers[key][chunkIndex] = rows;

      console.log(`📦 รับ chunk ${chunkIndex + 1}/${chunkTotal} (${rows.length} rows)`);

      if (chunkBuffers[key].filter(Boolean).length === chunkTotal) {
        const allRows = chunkBuffers[key].flat();
        delete chunkBuffers[key];

        console.log(`✅ รวมครบ ${allRows.length} rows สำหรับ ${queryName}/${hosCode}`);
        if (!Array.isArray(allRows) || allRows.length === 0) {
          console.warn(`⚠️ allRows ว่างสำหรับ ${queryName}/${hosCode}`);
          return;
        }

        const sampleRow = allRows[0];
        await ensureTable(queryName, sampleRow);

        await pool.query("TRUNCATE TABLE `" + queryName + "`");

        const columns = Object.keys(sampleRow);
        const colNames = columns.map(c => '`' + c + '`').join(',');
        const values = allRows.map(row => columns.map(c => row[c]));
        const placeholders = values.map(v => '(' + v.map(() => '?').join(',') + ')').join(',');

        const sql = "INSERT INTO `" + queryName + "` (" + colNames + ") VALUES " + placeholders;

        try {
          const [result] = await pool.query(sql, values.flat());
          console.log(`✅ Inserted ${result.affectedRows} rows into ${queryName}`);
        } catch (err) {
          console.error(`❌ Insert error on ${queryName}:`, err.message);
          console.error(`SQL: ${sql}`);
        }

        socket.emit('dataResponse', { message: 'Data inserted successfully', rows: allRows.length });
      }
    } catch (err) {
      console.error('❌ clientData error:', err.message);
      socket.emit('dataResponse', { message: 'Error on clientData', error: err.message });
    }
  });

  socket.on('disconnect', () => {
    for (let hc in clients) {
      if (clients[hc] === socket) {
        delete clients[hc];
        console.log(`Client ${hc} disconnected`);
        break;
      }
    }
  });
});

/* ========== Trigger ให้ client ทำงาน (serverCommand) ========== */
app.post('/query/:queryName/:hosCode', async (req, res) => {
  const { queryName, hosCode } = req.params;
  const paramsArray = Object.entries(req.query).map(([key, value]) => ({ key, value }));
  const silent = req.query.silent === 'true' || req.query.silent === true;
  const hisType = req.query.hisType || 'hosxpv3';

  if (!clients[hosCode]) {
    return res.status(404).send(`Client ${hosCode} not found`);
  }

  try {
    const sql = await fetchSqlTemplate(hisType, queryName);
    clients[hosCode].emit('serverCommand', { queryName, hosCode, sql, params: paramsArray, hisType, silent });
    console.log(`📡 สั่ง client ${hosCode} รัน ${queryName} [hisType=${hisType}]`);
    res.status(200).send(`ส่งคำสั่งไปยัง ${hosCode}`);
  } catch (err) {
    console.error(`❌ fetchSqlTemplate error:`, err.message);
    res.status(500).send(`❌ ล้มเหลว: ${err.message}`);
  }
});

/* ========== ลบตาราง ========== */
app.post('/delete-query/:queryName', async (req, res) => {
  const queryName = req.params.queryName?.replace(/[^a-zA-Z0-9_]/g, '');
  if (!queryName) return res.status(400).send('❌ queryName ไม่ถูกต้อง');
  try {
    await pool.query(`DROP TABLE IF EXISTS \`${queryName}\``);
    console.log(`🧹 ลบตาราง ${queryName} แล้ว`);
    res.send(`✅ ลบตาราง ${queryName} สำเร็จ`);
  } catch (err) {
    console.error(`❌ ลบตาราง ${queryName} ล้มเหลว:`, err.message);
    res.status(500).send(`❌ ล้มเหลว: ${err.message}`);
  }
});

/* ========== ดึงข้อมูลย้อนหลัง ========== */
app.get('/data/:queryName/:hosCode', async (req, res) => {
  const { queryName, hosCode } = req.params;
  const { startDate, endDate } = req.query;

  if (!(await tableExists(queryName))) {
    return res.status(404).json({ status: 'deleted', error: `API ${queryName} ถูกลบแล้ว` });
  }

  let sql = `SELECT * FROM \`${queryName}\` WHERE hoscode = ?`;
  const params = [hosCode];
  if (startDate && endDate) {
    sql += ` AND vstdate BETWEEN ? AND ?`;
    params.push(startDate, endDate);
  }

  try {
    const [rows] = await pool.execute(sql, params);
    res.json(rows);
  } catch (error) {
    console.error('❌ ดึงข้อมูลย้อนหลังล้มเหลว:', error.message);
    res.status(500).send('ดึงข้อมูลไม่สำเร็จ');
  }
});

/* ========== ดึงข้อมูลล่าสุดสำหรับ Looker Studio ========== */
app.get('/query/:queryName/:hosCode', async (req, res) => {
  const { queryName, hosCode } = req.params;
  const apiKey = req.query.key;
  const isSummary = req.query.summary === 'true';
  const isPing = req.query.ping === 'true' || req.query.check === '1';

  if (isPing) {
    return res.json({ status: 'ok', message: `API connected: ${queryName}/${hosCode}` });
  }

  if (!(await tableExists(queryName))) {
    return res.status(404).json({ status: 'deleted', error: `API ${queryName} ถูกลบแล้ว` });
  }

  if (isSummary) {
    if (apiKey !== process.env.API_KEY_SUMMARY) {
      return res.status(401).json({ error: 'Unauthorized: summary key invalid' });
    }
    try {
      const [rows] = await pool.execute(
        `SELECT COUNT(*) AS count FROM \`${queryName}\` WHERE hoscode = ?`,
        [hosCode]
      );
      return res.json({
        status: 'ok',
        queryName,
        hosCode,
        rowCount: rows[0].count,
        checkedAt: new Date().toISOString()
      });
    } catch (err) {
      return res.status(500).json({ status: 'error', queryName, hosCode, message: err.message });
    }
  }

  if (apiKey !== process.env.API_KEY) {
    return res.status(401).json({ error: 'Unauthorized: data access key invalid' });
  }

  try {
    const [rows] = await pool.execute(
      `SELECT * FROM \`${queryName}\` WHERE hoscode = ? ORDER BY id DESC`,
      [hosCode]
    );
    return res.json(rows);
  } catch (error) {
    console.error('❌ ดึงข้อมูลล้มเหลว:', error.message);
    res.status(500).json({ error: 'ไม่สามารถดึงข้อมูลได้' });
  }
});

/* ========== Cron Jobs ========== */
async function loadCronJobsFromDB() {
  try {
    const [rows] = await cronDB.query(`
      SELECT sq.query_name, sq.hos_code, cp.cron_expr, cp.label, cp.notify_mode
      FROM save_query sq
      JOIN cron_profiles cp ON sq.cron_id = cp.id
      WHERE cp.cron_expr IS NOT NULL AND cp.cron_expr <> ''
    `);

    console.log('🔍 cron rows ที่โหลดได้:', rows.length);

    const grouped = {};
    for (const row of rows) {
      const expr = row.cron_expr;
      if (!grouped[expr]) {
        grouped[expr] = {
          label: row.label || `DB-CRON ${expr}`,
          notifyMode: row.notify_mode || 'ALL',
          tasks: []
        };
      }
      grouped[expr].tasks.push({
        queryName: row.query_name,
        hosCode: row.hos_code
      });
    }

    for (const cronTime in grouped) {
      const { label, tasks } = grouped[cronTime];
      console.log(`📆 ตั้งเวลา ${label} →`, tasks.map(t => `${t.queryName}/${t.hosCode}`));
      cron.schedule(cronTime, async () => {
        for (const { queryName, hosCode } of tasks) {
          try {
            await fetch(`http://localhost:${PORT}/query/${queryName}/${hosCode}`, { method: 'POST' });
            console.log(`[${label}] ✅ Triggered ${queryName}/${hosCode}`);
          } catch (err) {
            console.error(`[${label}] ❌ ${queryName}/${hosCode}:`, err.message);
          }
        }
      });
    }
  } catch (err) {
    console.error('❌ โหลด cron jobs จาก DB ล้มเหลว:', err.message);
  }
}

/* ========== Start Server ========== */
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', async () => {
  console.log(`🚀 Server พร้อมใช้งานที่ port ${PORT}`);
  await loadCronJobsFromDB();
});