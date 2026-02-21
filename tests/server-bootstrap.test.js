"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { spawn } = require("node:child_process");

const PROJECT_ROOT = path.resolve(__dirname, "..");

function runNodeScript(scriptSource, { timeoutMs = 8000 } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["-e", scriptSource], {
      cwd: PROJECT_ROOT,
      env: {
        ...process.env,
        NODE_ENV: "test",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    let timedOut = false;

    const timeoutId = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, timeoutMs);

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("error", (error) => {
      clearTimeout(timeoutId);
      reject(error);
    });

    child.on("close", (code, signal) => {
      clearTimeout(timeoutId);
      resolve({
        code,
        signal,
        stdout,
        stderr,
        timedOut,
      });
    });
  });
}

test("server.js exports app/startServer without auto-listen in test mode", async () => {
  const script = `
    const serverModule = require("./server.js");
    if (!serverModule || typeof serverModule !== "object") {
      throw new Error("server module export is missing");
    }
    if (typeof serverModule.startServer !== "function") {
      throw new Error("startServer export is missing");
    }
    if (!serverModule.app || typeof serverModule.app.listen !== "function") {
      throw new Error("express app export is missing");
    }
    console.log("SERVER_EXPORTS_OK");
  `;

  const result = await runNodeScript(script);

  assert.equal(
    result.timedOut,
    false,
    `Child process timed out while importing server.js. stderr:\n${result.stderr}`,
  );
  assert.equal(result.code, 0, `Child process exited with code ${result.code}. stderr:\n${result.stderr}`);
  assert.match(result.stdout, /SERVER_EXPORTS_OK/);
});
