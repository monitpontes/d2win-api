// src/services/s3Objects.js
import { ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3";
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