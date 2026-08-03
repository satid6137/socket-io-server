// nodejs-server.js
import cron from "node-cron";
import express from "express";
import http from "http";
import { Server } from "socket.io";
import mysql from "mysql2/promise";
import dotenv from "dotenv";
import cors from "cors";
import fetch from "node-fetch";
import zlib from "zlib";
import fs from "fs";

// ⭐ Global cron storage
let cronJobs = [];   // เก็บ cron jobs ที่สร้างทั้งหมด

dotenv.config();

/* ========== CONFIG ========== */
const phpApiUrl = process.env.PHP_API_URL;
console.log("phpApiUrl =", phpApiUrl);

/* ========== Notify: MOPH + LINE + Dynamic ========== */

// MOPH Notify (NEW API) - ใช้ได้ทั้ง global (.env) และ per-query (จาก DB)
async function sendMophNotify(clientKey, secretKey, textMessage) {
  if (!clientKey || !secretKey) {
    console.warn("⚠️ sendMophNotify ถูกเรียกแต่ไม่มี clientKey/secretKey");
    return null;
  }

  try {
    const notifyUrl = `${process.env.MOPH_NOTIFY_URL}/api/notify/send`;

    const body = {
      messages: [
        {
          type: "text",
          text: textMessage,
        },
      ],
    };

    const res = await fetch(notifyUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "client-key": clientKey,
        "secret-key": secretKey,
      },
      body: JSON.stringify(body),
    });

    const data = await res.json();

    if (data.message_code !== 200 && data.status !== 200) {
      console.error("❌ ส่ง MOPH Notify ไม่สำเร็จ:", data);
    } else {
      console.log("📨 ส่ง MOPH Notify สำเร็จ:", textMessage);
    }
    return data;
  } catch (err) {
    console.error("❌ MOPH Notify error:", err.message);
    return null;
  }
}

async function sendLineNotify(token, message) {
  if (!token) {
    console.warn("⚠️ sendLineNotify ถูกเรียกแต่ไม่มี token");
    return null;
  }

  try {
    const res = await fetch("https://notify-api.line.me/api/notify", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: new URLSearchParams({ message }),
    });

    const data = await res.json();
    console.log("📨 LINE Notify:", data);
    return data;
  } catch (err) {
    console.error("❌ LINE Notify error:", err);
    return null;
  }
}

// ดึง notify config จาก PHP (notify_settings)
async function fetchNotifyConfig(queryName, hosCode) {
  try {
    const url = `${phpApiUrl}?notify=1&queryName=${encodeURIComponent(queryName)}&hosCode=${encodeURIComponent(hosCode)}`;
    const res = await fetch(url, { timeout: 5000 });
    const data = await res.json();

    if (!data || !data.notify) {
      return { notify_type: "none" };
    }

    return data.notify;
  } catch (err) {
    console.error("❌ fetchNotifyConfig error:", err.message);
    return { notify_type: "none" };
  }
}

// เลือกส่ง notify ตาม notify_type จาก notify_settings
async function sendDynamicNotify(notifyConfig, message) {
  if (!notifyConfig || notifyConfig.notify_type === "none") {
    return;
  }

  if (notifyConfig.notify_type === "line") {
    return await sendLineNotify(notifyConfig.line_token, message);
  }

  if (notifyConfig.notify_type === "moph") {
    return await sendMophNotify(
      notifyConfig.moph_client_key,
      notifyConfig.moph_secret_key,
      message,
    );
  }
}

/* ========== Pools ========== */
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: process.env.DB_PORT,
  charset: "utf8mb4",
});

const cronDB = mysql.createPool({
  host: process.env.CRON_DB_HOST,
  user: process.env.CRON_DB_USER,
  password: process.env.CRON_DB_PASSWORD,
  database: process.env.CRON_DB_NAME,
  port: process.env.CRON_DB_PORT,
  charset: "utf8mb4",
});

/* ========== Utility functions ========== */
async function tableExists(queryName) {
  const [rows] = await pool.query(`SHOW TABLES LIKE ?`, [queryName]);
  return rows.length > 0;
}

async function fetchSqlTemplate(hisType, queryName) {
  const url = `${phpApiUrl}?hisType=${encodeURIComponent(hisType)}&queryName=${encodeURIComponent(queryName)}`;
  const res = await fetch(url, { timeout: 20000 });
  if (!res.ok)
    throw new Error(`PHP API error: ${res.status} ${res.statusText}`);
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
  ? process.env.ALLOWED_ORIGINS.split(",").map((origin) => origin.trim())
  : [];

app.use(
  cors({
    origin: function (origin, callback) {
      if (!origin || allowedOrigins.includes(origin)) {
        callback(null, true);
      } else {
        callback(new Error("⛔ Origin ไม่ได้รับอนุญาต"));
      }
    },
  }),
);

let clients = {};
const chunkBuffers = {};

/* ========== Ensure table (สร้างครั้งแรกเท่านั้น) ========== */
async function ensureTable(queryName, sampleRow) {
  const [rows] = await pool.query(`SHOW TABLES LIKE ?`, [queryName]);
  if (rows.length > 0) return;

  const uniqueKeys = [...new Set(Object.keys(sampleRow))];
  const columnsDef = uniqueKeys
    .map((k) => `\`${k.replace(/`/g, "")}\` TEXT`)
    .join(", ");

  const createSQL = `
    CREATE TABLE \`${queryName}\` (
      id INT AUTO_INCREMENT PRIMARY KEY,
      ${columnsDef},
      last_update DATETIME
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
  `;
  await pool.query(createSQL);
  console.log(`🧱 Created table ${queryName}`);
}

/* ========== Socket events ========== */
io.on("connection", (socket) => {
  socket.on("register", (clientInfo) => {
    const { hosCode, hosName } = clientInfo;
    clients[hosCode] = socket;
    console.log(
      `✅ REGISTER hosCode=${hosCode}, hosName=${hosName}, socket=${socket.id}`,
    );
    socket.emit("greeting", `<-- Server: ยินดีต้อนรับ [${hosCode}]`);
  });

  socket.on("clientMetric", (metric) => {
    console.log("📊 clientMetric:", metric);
  });

  socket.on("clientData", async (data) => {
    const {
      hosCode,
      queryName,
      data: payload,
      compressed,
      chunkIndex,
      chunkTotal,
    } = data;

    if (!clients[hosCode]) {
      console.warn(`⚠️ clientData ก่อน register: ${hosCode}`);
      socket.emit("error", "Client not registered");
      return;
    }

    try {
      let jsonStr;
      if (compressed) {
        const buf = Buffer.from(payload, "base64");
        jsonStr = zlib.gunzipSync(buf).toString("utf8");
      } else {
        jsonStr = payload;
      }
      const rows = JSON.parse(jsonStr);

      const key = `${hosCode}_${queryName}`;
      if (!chunkBuffers[key]) chunkBuffers[key] = [];
      chunkBuffers[key][chunkIndex] = rows;

      console.log(
        `📦 รับ chunk ${chunkIndex + 1}/${chunkTotal} (${rows.length} rows) สำหรับ ${queryName}/${hosCode}`,
      );

      if (chunkBuffers[key].filter(Boolean).length === chunkTotal) {
        const allRows = chunkBuffers[key].flat();
        delete chunkBuffers[key];

        console.log(
          `✅ รวมครบ ${allRows.length} rows สำหรับ ${queryName}/${hosCode}`,
        );
        if (!Array.isArray(allRows) || allRows.length === 0) {
          console.warn(
            `ℹ️ allRows ว่างสำหรับ ${queryName}/${hosCode} → ensureTable + TRUNCATE + last_update`,
          );

          // ⭐ สร้าง table ถ้ายังไม่มี (empty-case ก็ต้องสร้าง)
          const emptySample = { last_update: "" };
          await ensureTable(queryName, emptySample);

          // ⭐ ล้างข้อมูลเก่า
          await pool.query(`TRUNCATE TABLE \`${queryName}\``);

          // ⭐ บันทึก last_update
          await pool.query(
            `INSERT INTO \`${queryName}\` (last_update) VALUES (NOW())`,
          );

          socket.emit("dataResponse", {
            message: "Empty result saved",
            rows: 0,
          });

          return;
        }

        const sampleRow = allRows[0];
        await ensureTable(queryName, sampleRow);

        await pool.query("TRUNCATE TABLE `" + queryName + "`");

        const columns = [...Object.keys(sampleRow), "last_update"];
        const colNames = columns.map((c) => "`" + c + "`").join(",");
        const values = allRows.map((row) => [
          ...Object.keys(sampleRow).map((c) => row[c]),
          new Date(),
        ]);

        const placeholders = values
          .map((v) => "(" + v.map(() => "?").join(",") + ")")
          .join(",");

        const sql =
          "INSERT INTO `" +
          queryName +
          "` (" +
          colNames +
          ") VALUES " +
          placeholders;

        try {
          const [result] = await pool.query(sql, values.flat());
          console.log(
            `✅ Inserted ${result.affectedRows} rows into ${queryName}/${hosCode}`,
          );
        } catch (err) {
          console.error(
            `❌ Insert error on ${queryName}/${hosCode}:`,
            err.message,
          );
          console.error(`SQL: ${sql}`);

          // แจ้งเตือน error แบบ global
          if (process.env.MOPH_CLIENT_KEY && process.env.MOPH_SECRET_KEY) {
            await sendMophNotify(
              process.env.MOPH_CLIENT_KEY,
              process.env.MOPH_SECRET_KEY,
              `❌ Insert error on ${queryName}/${hosCode}: ${err.message}`,
            );
          }
        }

        socket.emit("dataResponse", {
          message: "Data inserted successfully",
          rows: allRows.length,
        });

        // ถ้าต้องการแจ้งเตือนเมื่อรับข้อมูลสำเร็จ (ต่อ query):
        // const notifyConfig = await fetchNotifyConfig(queryName, hosCode);
        // await sendDynamicNotify(notifyConfig, `📥 รับข้อมูลจาก ${hosCode}/${queryName} จำนวน ${allRows.length} rows`);
      }
    } catch (err) {
      console.error(
        `❌ clientData error on ${queryName}/${hosCode}:`,
        err.message,
      );

      // 🔥 ล้าง buffer ที่ค้างอยู่
      const key = `${hosCode}_${queryName}`;
      if (chunkBuffers[key]) {
        delete chunkBuffers[key];
        console.log(`🧹 เคลียร์ chunkBuffers ของ ${queryName}/${hosCode}`);
      }

      socket.emit("dataResponse", {
        message: "Error on clientData",
        error: err.message,
      });

      // ⭐ ไม่ต้องส่ง LINE ถ้าเป็น error ที่เกิดจาก empty-case หรือโครงสร้างตาราง
      const ignoreErrors = [
        "Duplicate column name",
        "Unknown column",
        "doesn't exist",
        "ER_NO_SUCH_TABLE",
        "ER_BAD_FIELD_ERROR",
      ];

      if (ignoreErrors.some((txt) => err.message.includes(txt))) {
        console.log(
          `ℹ️ clientData error ถูกข้าม (ไม่ส่ง LINE): ${err.message}`,
        );
        return;
      }

      // ⭐ error อื่น ๆ ค่อยส่ง LINE
      if (process.env.MOPH_CLIENT_KEY && process.env.MOPH_SECRET_KEY) {
        await sendMophNotify(
          process.env.MOPH_CLIENT_KEY,
          process.env.MOPH_SECRET_KEY,
          `❌ clientData error on ${queryName}/${hosCode}: ${err.message}`,
        );
      }
    }
  });

  socket.on("disconnect", () => {
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
app.post("/query/:queryName/:hosCode", async (req, res) => {
  const { queryName, hosCode } = req.params;
  const paramsArray = Object.entries(req.query).map(([key, value]) => ({
    key,
    value,
  }));
  const silent = req.query.silent === "true" || req.query.silent === true;
  const hisType = req.headers["x-his-type"] || req.query.hisType || "hosxpv3";

  if (!clients[hosCode]) {
    return res.status(404).send(`Client ${hosCode} not found`);
  }

  try {
    const sql = await fetchSqlTemplate(hisType, queryName);
    clients[hosCode].emit("serverCommand", {
      queryName,
      hosCode,
      sql,
      params: paramsArray,
      hisType,
      silent,
    });
    console.log(
      `📡 สั่ง client ${hosCode} รัน ${queryName} [hisType=${hisType}]`,
    );
    res.status(200).send(`ส่งคำสั่งไปยัง ${hosCode}`);
  } catch (err) {
    console.error(`❌ fetchSqlTemplate error:`, err.message);
    res.status(500).send(`❌ ล้มเหลว: ${err.message}`);
  }
});

/* ========== ส่ง line ========== */
app.post("/send-notify-now", express.json(), async (req, res) => {
  // ⭐ ต้องรับ notify มาด้วย
  const { queryName, hosCode, notify } = req.body;

  console.log("📨 ส่งแจ้งเตือนทันที:", queryName, hosCode);
  console.log("📌 notify:", notify);

  try {
    // 1) โหลด notify config จากฐาน
    const notifyConfig = await fetchNotifyConfig(queryName, hosCode);
    if (!notifyConfig || notifyConfig.notify_type === "none") {
      return res.json({ success: false, error: "ยังไม่ได้กำหนดการแจ้งเตือน" });
    }

    // 2) ดึงข้อมูลจากตาราง
    const [rows] = await pool.query(
      `SELECT * FROM \`${queryName}\` WHERE hoscode = ? ORDER BY id ASC`,
      [hosCode],
    );

    if (!rows || rows.length === 0) {
      return res.json({ success: false, error: "ไม่มีข้อมูลในตาราง" });
    }

    // 3) สร้างข้อความแจ้งเตือน
    let msg = "";

    // ⭐ ป้องกัน notify undefined
    if (notify && notify.description) {
      msg += `📌 ${notify.description}\n\n`;
    }

    // ⭐ แก้ตัวแปรผิดชื่อ
    msg += `📊 แจ้งเตือนตามเวลา: ${queryName}/${hosCode}\n`;
    msg += `จำนวนทั้งหมด ${rows.length} รายการ\n\n`;

    rows.forEach((r, i) => {
      msg += `#${i + 1}\n`;

      for (const col in r) {
        msg += `• ${col}: ${r[col]}\n`;
      }

      msg += `\n`;
    });

    // 4) ส่งแจ้งเตือน
    await sendDynamicNotify(notifyConfig, msg);

    res.json({ success: true, message: "ส่งแจ้งเตือนสำเร็จ" });
  } catch (err) {
    res.json({ success: false, error: err.message });
  }
});

/* ========== ลบตาราง (เวอร์ชันแก้สมบูรณ์) ========== */
app.post("/delete-query/:queryName", async (req, res) => {
	res.setHeader("Content-Type", "application/json; charset=utf-8");
	
  try {
    // ❗ ใช้ชื่อจริงที่ PHP ส่งมา ไม่ sanitize ทิ้ง - หรือ .
    let queryName = req.params.queryName;

    if (!queryName || typeof queryName !== "string") {
      return res.status(400).json({
        success: false,
        error: "❌ queryName ไม่ถูกต้อง",
      });
    }

    // ป้องกัน SQL injection (ลบเฉพาะ backtick)
    const safeName = queryName.replace(/`/g, "");

    console.log(`🧹 คำขอลบตารางจาก PHP: ${safeName}`);

    // ตรวจว่าตารางมีจริงไหม
    const [rows] = await pool.query(`SHOW TABLES LIKE ?`, [safeName]);
    if (rows.length === 0) {
      console.log(`ℹ️ ตาราง ${safeName} ไม่มีอยู่แล้ว`);
      return res.json({
        success: true,
        message: `ℹ️ ตาราง ${safeName} ไม่มีอยู่แล้ว`,
      });
    }

    // ลบตารางจริง
    await pool.query(`DROP TABLE IF EXISTS \`${safeName}\``);
    console.log(`🧹 ลบตาราง ${safeName} สำเร็จ`);

    // ส่ง Notify (ถ้ามี key)
    if (process.env.MOPH_CLIENT_KEY && process.env.MOPH_SECRET_KEY) {
      await sendMophNotify(
        process.env.MOPH_CLIENT_KEY,
        process.env.MOPH_SECRET_KEY,
        `🧹 ลบตาราง ${safeName} สำเร็จ`,
      );
    }

    return res.json({
      success: true,
      message: `✅ ลบตาราง ${safeName} สำเร็จ`,
    });
  } catch (err) {
    console.error(`❌ delete-query error:`, err.message);

    if (process.env.MOPH_CLIENT_KEY && process.env.MOPH_SECRET_KEY) {
      await sendMophNotify(
        process.env.MOPH_CLIENT_KEY,
        process.env.MOPH_SECRET_KEY,
        `❌ ลบตารางล้มเหลว: ${err.message}`,
      );
    }

    return res.status(500).json({
      success: false,
      error: `❌ ล้มเหลว: ${err.message}`,
    });
  }
});

/* ========== เพิ่ม endpoint ให้ PHP เรียก reload cron========== */
app.post("/reload-cron", async (req, res) => {
  try {
    await loadCronJobsFromDB();
    res.json({ success: true, message: "cron reloaded" });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});



/* ========== ดึงข้อมูลย้อนหลัง ========== */
app.get("/data/:queryName/:hosCode", async (req, res) => {
  const { queryName, hosCode } = req.params;
  const { startDate, endDate } = req.query;

  if (!(await tableExists(queryName))) {
    return res
      .status(404)
      .json({ status: "deleted", error: `API ${queryName} ถูกลบแล้ว` });
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
    console.error("❌ ดึงข้อมูลย้อนหลังล้มเหลว:", error.message);
    res.status(500).send("ดึงข้อมูลไม่สำเร็จ");
  }
});

/* ========== ดึงข้อมูลล่าสุดสำหรับ Looker Studio ========== */
app.get("/query/:queryName/:hosCode", async (req, res) => {
  const { queryName, hosCode } = req.params;
  const apiKey = req.headers["x-api-key"] || req.query.key;
  const isSummary = req.query.summary === "true";
  const isPing = req.query.ping === "true" || req.query.check === "1";

  if (isPing) {
    return res.json({
      status: "ok",
      message: `API connected: ${queryName}/${hosCode}`,
    });
  }

  if (!(await tableExists(queryName))) {
    return res
      .status(404)
      .json({ status: "deleted", error: `API ${queryName} ถูกลบแล้ว` });
  }

  if (isSummary) {
    if (apiKey !== process.env.API_KEY_SUMMARY) {
      return res
        .status(401)
        .json({ error: "Unauthorized: summary key invalid" });
    }
    try {
      const [rows] = await pool.execute(
        `SELECT COUNT(*) AS count FROM \`${queryName}\` WHERE hoscode = ?`,
        [hosCode],
      );
      return res.json({
        status: "ok",
        queryName,
        hosCode,
        rowCount: rows[0].count,
        checkedAt: new Date().toISOString(),
      });
    } catch (err) {
      return res
        .status(500)
        .json({ status: "error", queryName, hosCode, message: err.message });
    }
  }

  if (apiKey !== process.env.API_KEY) {
    return res
      .status(401)
      .json({ error: "Unauthorized: data access key invalid" });
  }

  try {
    const [rows] = await pool.execute(
      `SELECT * FROM \`${queryName}\` WHERE hoscode = ? ORDER BY id DESC`,
      [hosCode],
    );
    return res.json(rows);
  } catch (error) {
    console.error("❌ ดึงข้อมูลล้มเหลว:", error.message);
    res.status(500).json({ error: "ไม่สามารถดึงข้อมูลได้" });
  }
});

/* ========== Cron Jobs ========== */
async function loadCronJobsFromDB() {
  try {
    console.log("🔄 Reloading cron jobs from DB...");

    // 1) หยุด cron เดิมทั้งหมดก่อน
    cronJobs.forEach(job => job.stop());
    cronJobs = [];

    // 2) โหลด cron สำหรับ save_query
    const [rows] = await cronDB.query(`
      SELECT sq.query_name, sq.hos_code, cp.cron_expr, cp.label, cp.notify_mode
      FROM save_query sq
      JOIN cron_profiles cp ON sq.cron_id = cp.id
      WHERE cp.cron_expr IS NOT NULL AND cp.cron_expr <> ''
    `);

    const grouped = {};
    for (const row of rows) {
      const expr = row.cron_expr;
      if (!grouped[expr]) {
        grouped[expr] = {
          label: row.label || `DB-CRON ${expr}`,
          notifyMode: row.notify_mode || "ALL",
          tasks: [],
        };
      }
      grouped[expr].tasks.push({
        queryName: row.query_name,
        hosCode: row.hos_code,
      });
    }

    for (const cronTime in grouped) {
      const { label, tasks } = grouped[cronTime];

      const job = cron.schedule(cronTime, async () => {
        for (const { queryName, hosCode } of tasks) {
          try {
            const exists = await tableExists(queryName);
            if (exists) {
              await pool.query(
                `DELETE FROM \`${queryName}\` WHERE hoscode = ?`,
                [hosCode],
              );
            }

            await fetch(
              `http://localhost:${PORT}/query/${queryName}/${hosCode}`,
              { method: "POST" },
            );

            console.log(`[${label}] Triggered ${queryName}/${hosCode}`);
          } catch (err) {
            console.error(`[${label}] error:`, err.message);
          }
        }
      });

      cronJobs.push(job);
    }

    // 3) โหลด cron สำหรับ notify_settings
    const [notifyRows] = await cronDB.query(`
      SELECT ns.query_name, ns.hos_code, cp.cron_expr, cp.label
      FROM notify_settings ns
      JOIN cron_profiles cp ON ns.cron_id = cp.id
      WHERE cp.cron_expr IS NOT NULL AND cp.cron_expr <> ''
    `);

    for (const row of notifyRows) {
  const job = cron.schedule(row.cron_expr, async () => {
    try {
      const notifyConfig = await fetchNotifyConfig(row.query_name, row.hos_code);
      if (!notifyConfig || notifyConfig.notify_type === "none") {
        console.log(`ℹ️ ไม่มี notify_type สำหรับ ${row.query_name}/${row.hos_code}`);
        return;
      }

      const exists = await tableExists(row.query_name);
      if (!exists) {
        console.log(`ℹ️ notify cron: ตาราง ${row.query_name} ยังไม่ถูกสร้าง`);
        return;
      }

      const [rows] = await pool.query(
        `SELECT * FROM \`${row.query_name}\` WHERE hoscode = ? ORDER BY id ASC`,
        [row.hos_code]
      );

      if (!rows || rows.length === 0) return;

      let msg = "";

      // ⭐ ใช้ notifyConfig.description เหมือน manual
      if (notifyConfig.description) {
        msg += `📌 ${notifyConfig.description}\n\n`;
      }

      msg += `📊 แจ้งเตือนตามเวลา: ${row.query_name}/${row.hos_code}\n`;
      msg += `จำนวนทั้งหมด ${rows.length} รายการ\n\n`;

      rows.forEach((r, i) => {
        msg += `#${i + 1}\n`;
        for (const col in r) {
          msg += `• ${col}: ${r[col]}\n`;
        }
        msg += `\n`;
      });

      await sendDynamicNotify(notifyConfig, msg);

      console.log(`🔔 ส่งแจ้งเตือนตามเวลา: ${row.query_name}/${row.hos_code}`);
    } catch (err) {
      console.error(`❌ notify cron error:`, err.message);
    }
  });

  cronJobs.push(job);
}


    console.log(`✅ cron loaded: ${cronJobs.length} jobs`);

  } catch (err) {
    console.error("❌ loadCronJobsFromDB error:", err.message);
  }
}

/* ========== Start Server ========== */
const PORT = process.env.PORT || 3000;
server.listen(PORT, "0.0.0.0", async () => {
  console.log(`🚀 Server พร้อมใช้งานที่ port ${PORT}`);
  await loadCronJobsFromDB();
});
