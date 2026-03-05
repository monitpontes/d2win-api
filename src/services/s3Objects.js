// src/services/s3Objects.js
import {
  ListObjectsV2Command,
  GetObjectCommand,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";
import { getS3Client } from "./s3Client.js";

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`${name} ausente no .env`);
  return v;
}

export async function listPrefix(prefix) {
  const Bucket = mustEnv("S3_BUCKET");
  const s3 = getS3Client();

  const out = [];
  let ContinuationToken = undefined;

  do {
    const resp = await s3.send(
      new ListObjectsV2Command({
        Bucket,
        Prefix: prefix,
        ContinuationToken,
      })
    );

    for (const o of resp.Contents || []) {
      out.push({
        key: o.Key,
        size: o.Size,
        lastModified: o.LastModified,
      });
    }

    ContinuationToken = resp.IsTruncated ? resp.NextContinuationToken : undefined;
  } while (ContinuationToken);

  return out;
}

// ✅ NOVO: checa se um objeto existe no S3 (rápido, não baixa nada)
export async function existsObject(key) {
  const Bucket = mustEnv("S3_BUCKET");
  const s3 = getS3Client();

  try {
    await s3.send(new HeadObjectCommand({ Bucket, Key: key }));
    return true;
  } catch (e) {
    // quando não existe, geralmente vem 404
    if (e?.$metadata?.httpStatusCode === 404) return false;
    // alguns casos retornam name/code
    if (e?.name === "NotFound" || e?.Code === "NotFound") return false;
    throw e;
  }
}

// Converte stream do S3 em Buffer
async function streamToBuffer(readable) {
  const chunks = [];
  for await (const chunk of readable) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
}

export async function getObjectBuffer(key) {
  const Bucket = mustEnv("S3_BUCKET");
  const s3 = getS3Client();

  const resp = await s3.send(new GetObjectCommand({ Bucket, Key: key }));
  if (!resp.Body) throw new Error("S3 GetObject sem Body");
  return streamToBuffer(resp.Body);
}