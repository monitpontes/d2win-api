// src/services/export/mongoToParquet.js
import mongoose from "mongoose";
import parquet from "parquetjs-lite";
import { Writable } from "node:stream";

/**
 * Converte um cursor do Mongo (find/aggregate cursor) em Buffer parquet.
 *
 * Uso (como você já faz):
 *   const cursor = db.collection(coll).find(query, { projection: { _id: 0 } });
 *   const buf = await cursorToParquetBuffer(cursor);
 *
 * IMPORTANTE:
 * - Este código normaliza valores para evitar crash do parquetjs-lite:
 *   ObjectId -> string
 *   Object/Array -> JSON.stringify
 *   Date -> Date (TIMESTAMP_MILLIS)
 *   undefined -> null
 */

// -------------------- Utils: stream -> buffer --------------------

class BufferCollector extends Writable {
  constructor() {
    super();
    this.chunks = [];
  }
  _write(chunk, enc, cb) {
    this.chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, enc));
    cb();
  }
  toBuffer() {
    return Buffer.concat(this.chunks);
  }
}

// -------------------- Normalização --------------------

function normalizeValue(v) {
  if (v === undefined || v === null) return null;

  // ObjectId -> string
  if (v instanceof mongoose.Types.ObjectId) return v.toString();

  // Date mantém Date (vamos usar TIMESTAMP_MILLIS)
  if (v instanceof Date) return v;

  // Buffer mantém Buffer
  if (Buffer.isBuffer(v)) return v;

  // Array/Object -> JSON string (evita o erro "Buffer.from(Object)")
  if (typeof v === "object") return JSON.stringify(v);

  // number/string/bool
  return v;
}

function normalizeRow(row) {
  const out = {};
  for (const [k, v] of Object.entries(row)) {
    out[k] = normalizeValue(v);
  }
  return out;
}

// -------------------- Inferência de schema --------------------

function parquetFieldFromValue(v) {
  // null não ajuda inferir; tratar como UTF8 opcional
  if (v === null) return { type: "UTF8", optional: true };

  // Date
  if (v instanceof Date) return { type: "TIMESTAMP_MILLIS", optional: true };

  // Buffer
  if (Buffer.isBuffer(v)) return { type: "BYTE_ARRAY", optional: true };

  // number
  if (typeof v === "number") {
    // Se for inteiro “seguro”, use INT64, senão DOUBLE
    if (Number.isInteger(v)) return { type: "INT64", optional: true };
    return { type: "DOUBLE", optional: true };
  }

  // boolean
  if (typeof v === "boolean") return { type: "BOOLEAN", optional: true };

  // string
  if (typeof v === "string") return { type: "UTF8", optional: true };

  // object/array (depois da normalização vira string JSON)
  return { type: "UTF8", optional: true };
}

function mergeFieldTypes(a, b) {
  // Se conflitar, sobe para UTF8 (mais seguro)
  if (!a) return b;
  if (!b) return a;
  if (a.type === b.type) return { ...a, optional: true };

  // INT64 + DOUBLE -> DOUBLE
  const nums = new Set([a.type, b.type]);
  if (nums.has("INT64") && nums.has("DOUBLE")) return { type: "DOUBLE", optional: true };

  // Qualquer conflito -> UTF8
  return { type: "UTF8", optional: true };
}

async function inferSchemaFromCursor(cursor, { sampleSize = 200 } = {}) {
  // lê alguns docs para inferir tipos (sem consumir o cursor original)
  // Estratégia: pega 1 doc com next() e depois continua usando o MESMO cursor,
  // mas guardamos os docs lidos num buffer para escrever antes.
  const buffered = [];

  let doc = await cursor.next();
  if (!doc) return { schema: new parquet.ParquetSchema({}), bufferedDocs: [] };

  buffered.push(doc);

  // coleta mais amostras
  for (let i = 1; i < sampleSize; i++) {
    const d = await cursor.next();
    if (!d) break;
    buffered.push(d);
  }

  // normaliza amostras e infere tipos
  const fields = {};
  for (const raw of buffered) {
    const row = normalizeRow(raw);

    for (const [k, v] of Object.entries(row)) {
      const f = parquetFieldFromValue(v);
      fields[k] = mergeFieldTypes(fields[k], f);
    }
  }

  // garante opcional
  for (const k of Object.keys(fields)) {
    fields[k].optional = true;
  }

  return { schema: new parquet.ParquetSchema(fields), bufferedDocs: buffered };
}

// -------------------- Função principal --------------------

export async function cursorToParquetBuffer(
  cursor,
  {
    // performance/tuning
    rowGroupSize = 50_000,
    sampleSize = 200,
    // se quiser forçar schema manual (pouco usado; auto é ok)
    schema: forcedSchema = null,
  } = {}
) {
  if (!cursor || typeof cursor.next !== "function") {
    throw new Error("cursorToParquetBuffer: cursor inválido (esperado Mongo cursor)");
  }

  // 1) schema
  let schema;
  let bufferedDocs = [];

  if (forcedSchema) {
    schema = forcedSchema instanceof parquet.ParquetSchema ? forcedSchema : new parquet.ParquetSchema(forcedSchema);
  } else {
    const inferred = await inferSchemaFromCursor(cursor, { sampleSize });
    schema = inferred.schema;
    bufferedDocs = inferred.bufferedDocs;
  }

  // 2) writer em memória
  const sink = new BufferCollector();

  // opções do parquetjs-lite (algumas versões suportam useDataPageV2)
  const writer = await parquet.ParquetWriter.openStream(schema, sink, {
    useDataPageV2: true,
    rowGroupSize,
  });

  let written = 0;

  try {
    // 3) escreve os docs bufferizados (que já foram consumidos no schema inference)
    for (const d of bufferedDocs) {
      const row = normalizeRow(d);
      // remove _id se tiver sobrado
      if (row._id !== undefined) delete row._id;

      await writer.appendRow(row);
      written++;
    }

    // 4) escreve o restante do cursor
    while (true) {
      const d = await cursor.next();
      if (!d) break;

      const row = normalizeRow(d);
      if (row._id !== undefined) delete row._id;

      await writer.appendRow(row);
      written++;
    }
  } catch (e) {
    // debug útil se voltar a quebrar
    console.error("[mongoToParquet] erro ao escrever parquet. written=", written);
    throw e;
  } finally {
    await writer.close();
  }

  return sink.toBuffer();
}