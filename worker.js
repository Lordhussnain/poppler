import { Worker, QueueScheduler } from "bullmq";
import IORedis from "ioredis";
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import fs from "fs-extra";
import tmp from "tmp-promise";
import { spawn } from "child_process";
import path from "path";
import pino from "pino";

const log = pino();
const redis = new IORedis(process.env.REDIS_URL || "redis://redis:6379");
new QueueScheduler(process.env.QUEUE_NAME || "poppler", { connection: redis });

const s3 = new S3Client({
  endpoint: process.env.S3_ENDPOINT,
  region: process.env.S3_REGION,
  credentials: { accessKeyId: process.env.S3_ACCESS_KEY, secretAccessKey: process.env.S3_SECRET_KEY },
  forcePathStyle: true,
});
const bucket = process.env.S3_BUCKET || "uploads";

async function download(key, outPath) {
  const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  await fs.ensureDir(path.dirname(outPath));
  await new Promise((resolve, reject) => {
    const ws = fs.createWriteStream(outPath);
    res.Body.pipe(ws).on("finish", resolve).on("error", reject);
  });
}

async function upload(localPath, key) {
  const body = fs.createReadStream(localPath);
  await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
}

function runCmd(cmd, args, opts = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, opts);
    let stderr = "";
    child.stderr.on("data", d => stderr += d.toString());
    child.on("close", code => code === 0 ? resolve() : reject(new Error(`${cmd} ${code} ${stderr}`)));
    child.on("error", reject);
  });
}

const worker = new Worker(process.env.QUEUE_NAME || "poppler", async job => {
  const { jobId, s3InputKey } = job.data;
  const tmpdir = await tmp.dir({ unsafeCleanup: true });
  try {
    const inputLocal = `${tmpdir.path}/input.pdf`;
    await download(s3InputKey, inputLocal);

    // example: render first page as png (thumbnail)
    const thumb = `${tmpdir.path}/thumb.png`;
    await runCmd("pdftoppm", ["-png", "-f", "1", "-singlefile", "-scale-to", "800", inputLocal, `${tmpdir.path}/thumb`]);
    // pdftoppm will write thumb.png
    await fs.pathExists(thumb) && await upload(thumb, `results/${jobId}/thumbnail.png`);

    // extract plain text
    const txtOut = `${tmpdir.path}/out.txt`;
    await runCmd("pdftotext", [inputLocal, txtOut]);
    await fs.pathExists(txtOut) && await upload(txtOut, `results/${jobId}/extracted.txt`);

    log.info({ jobId }, "poppler work done");
  } catch (err) {
    log.error({ err, jobId }, "poppler job failed");
    throw err;
  } finally {
    await tmpdir.cleanup().catch(()=>{});
  }
}, { connection: redis, concurrency: 2 });

worker.on("failed", (job, err) => log.warn({ id: job.id, err: err?.message }, "job failed"));
