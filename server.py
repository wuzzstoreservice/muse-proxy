import os
import sys
import json
import time
import uuid
import logging
import queue
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from patchright.sync_api import sync_playwright

HOST = "0.0.0.0"
PORT = 20133
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COOKIES_FILE = os.path.join(BASE_DIR, "cookies.txt")

# Auto-reset tab conversation after N requests to prevent memory leaks and keep accounts clean
MAX_SESSION_REQUESTS = 20

SUPPORTED_MODELS = [
    "muse-spark-1.3"
]

CHROMIUM_FLAGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-extensions",
    "--disable-background-networking",
    "--disable-background-timer-throttling",
    "--disable-breakpad",
    "--disable-default-apps",
    "--disable-features=Translate,BackForwardCache,AcceptCHFrame,MediaRouter,OptimizationHints",
    "--mute-audio",
    "--no-first-run",
    "--blink-settings=imagesEnabled=false"
]

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s"
)
logger = logging.getLogger("muse-proxy")


def parse_cookie_line(line):
    cookies = []
    pairs = line.split(";")
    for pair in pairs:
        pair = pair.strip()
        if not pair:
            continue
        parts = pair.split("=", 1)
        name = parts[0].strip()
        value = parts[1].strip() if len(parts) > 1 else ""
        cookies.append({
            "name": name,
            "value": value,
            "domain": ".muse.ai",
            "path": "/"
        })
    return cookies


def load_cookie_lines_from_file(file_path):
    if not os.path.exists(file_path):
        logger.warning(f"Cookies file not found: {file_path}")
        return []
    valid_lines = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                if "=" in stripped:
                    valid_lines.append(stripped)
    except Exception as e:
        logger.error(f"Error reading cookies file {file_path}: {e}")
    return valid_lines


class AccountSession:
    def __init__(self, idx, cookie_str, context, page):
        self.idx = idx
        self.cookie_str = cookie_str
        self.context = context
        self.page = page
        self.is_active = True
        self.consecutive_errors = 0
        self.total_requests = 0
        self.session_requests = 0

    def reset_chat_session(self):
        try:
            logger.info(f"Purging chat session & DOM cache for account #{self.idx}...")
            self.page.evaluate("""() => {
                try {
                    localStorage.removeItem('hatch-thread-titles');
                    localStorage.removeItem('hatch-last-seen-ts');
                    sessionStorage.clear();
                } catch(e) {}
            }""")
            self.page.goto("https://muse.ai/", wait_until="commit", timeout=30000)
            time.sleep(2)
            self.session_requests = 0
            logger.info(f"Account #{self.idx} chat session reset completed.")
        except Exception as e:
            logger.error(f"Error resetting chat session for account #{self.idx}: {e}")

    def close(self):
        try:
            if self.page:
                self.page.close()
        except Exception:
            pass
        try:
            if self.context:
                self.context.close()
        except Exception:
            pass


class MultiAccountBrowserWorker(threading.Thread):
    def __init__(self, cookies_path):
        super().__init__(daemon=True)
        self.cookies_path = cookies_path
        self.req_queue = queue.Queue()
        self.ready_event = threading.Event()
        self.playwright = None
        self.browser = None
        self.accounts = []
        self.rr_index = 0
        self.last_mtime = 0

    def run(self):
        logger.info("Initializing Ultra-Lightweight Playwright Chromium instance...")
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=True,
            args=CHROMIUM_FLAGS
        )

        self._sync_accounts()
        self.ready_event.set()

        while True:
            item = self.req_queue.get()
            if item is None:
                break
            action, payload, res_queue = item
            if action == "generate":
                prompt = payload
                try:
                    ans, acc_idx = self._execute_generate_with_failover(prompt)
                    res_queue.put(("ok", (ans, acc_idx)))
                except Exception as e:
                    logger.error(f"Generate failover error: {e}")
                    res_queue.put(("err", e))
            elif action == "reload":
                try:
                    count = self._sync_accounts(force=True)
                    res_queue.put(("ok", count))
                except Exception as e:
                    res_queue.put(("err", e))
            elif action == "reset":
                try:
                    for acc in self.accounts:
                        # Revive accounts paused by the consecutive-error circuit breaker.
                        if not acc.is_active:
                            acc.is_active = True
                            acc.consecutive_errors = 0
                            logger.info(f"Account #{acc.idx} re-activated by /reset.")
                        acc.reset_chat_session()
                    res_queue.put(("ok", len(self.accounts)))
                except Exception as e:
                    res_queue.put(("err", e))
            elif action == "status":
                status_info = {
                    "total_accounts": len(self.accounts),
                    "active_accounts": sum(1 for a in self.accounts if a.is_active),
                    "accounts": [
                        {
                            "index": a.idx,
                            "active": a.is_active,
                            "total_requests": a.total_requests,
                            "session_requests": a.session_requests,
                            "consecutive_errors": a.consecutive_errors
                        }
                        for a in self.accounts
                    ]
                }
                res_queue.put(("ok", status_info))

    def _sync_accounts(self, force=False):
        if not os.path.exists(self.cookies_path):
            logger.warning(f"No cookies file at {self.cookies_path}")
            return 0

        mtime = os.path.getmtime(self.cookies_path)
        if not force and mtime == self.last_mtime:
            return len(self.accounts)

        lines = load_cookie_lines_from_file(self.cookies_path)
        if not lines:
            logger.warning(f"No valid cookies found in {self.cookies_path}")
            return len(self.accounts)

        logger.info(f"Syncing accounts from cookies.txt ({len(lines)} line(s) detected)...")

        existing_cookie_map = {acc.cookie_str: acc for acc in self.accounts}
        new_accounts = []

        for idx, cookie_str in enumerate(lines):
            if cookie_str in existing_cookie_map:
                acc = existing_cookie_map.pop(cookie_str)
                acc.idx = idx
                new_accounts.append(acc)
            else:
                try:
                    logger.info(f"Spawning ultra-lightweight context for account #{idx}...")
                    ctx = self.browser.new_context(
                        viewport={"width": 800, "height": 600},
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
                    )
                    cookies = parse_cookie_line(cookie_str)
                    ctx.add_cookies(cookies)
                    page = ctx.new_page()
                    page.goto("https://muse.ai/", wait_until="commit", timeout=45000)
                    time.sleep(2)
                    logger.info(f"Account #{idx} ready: {page.title()}")
                    acc = AccountSession(idx, cookie_str, ctx, page)
                    new_accounts.append(acc)
                except Exception as e:
                    logger.error(f"Failed to initialize account #{idx}: {e}")

        for old_acc in existing_cookie_map.values():
            logger.info(f"Closing removed account #{old_acc.idx}")
            old_acc.close()

        self.accounts = new_accounts
        self.last_mtime = mtime
        logger.info(f"Account pool updated. Active accounts: {len(self.accounts)}")
        return len(self.accounts)

    def _execute_generate_with_failover(self, prompt):
        if os.path.exists(self.cookies_path):
            if os.path.getmtime(self.cookies_path) != self.last_mtime:
                self._sync_accounts()

        active = [a for a in self.accounts if a.is_active]
        if not active:
            self._sync_accounts(force=True)
            active = [a for a in self.accounts if a.is_active]
            if not active:
                raise Exception("No active Muse.ai accounts available in cookies.txt")

        num_accounts = len(active)
        start_idx = self.rr_index % num_accounts
        self.rr_index = (self.rr_index + 1) % num_accounts

        last_err = None
        for i in range(num_accounts):
            acc = active[(start_idx + i) % num_accounts]
            try:
                # If session has handled MAX_SESSION_REQUESTS, purge session before prompt
                if acc.session_requests >= MAX_SESSION_REQUESTS:
                    acc.reset_chat_session()

                logger.info(f"Dispatching prompt to account #{acc.idx} (attempt {i + 1}/{num_accounts})...")
                reply = self._generate_on_page(acc.page, prompt)
                acc.total_requests += 1
                acc.session_requests += 1
                acc.consecutive_errors = 0
                return reply, acc.idx
            except Exception as e:
                logger.error(f"Account #{acc.idx} failed: {e}")
                acc.consecutive_errors += 1
                if acc.consecutive_errors >= 3:
                    logger.warning(f"Account #{acc.idx} paused due to 3 consecutive errors (auto-resumes on /reset).")
                    acc.is_active = False
                last_err = e

        raise Exception(f"All {num_accounts} accounts in pool failed. Last error: {last_err}")

    def _generate_on_page(self, page, prompt):
        textarea = page.locator("textarea").first
        textarea.wait_for(state="visible", timeout=15000)

        # Capture a baseline of assistant text ALREADY on the page. Without this the
        # extractor matches the previous turn's reply and returns it as if it were the
        # answer to the current prompt (off-by-one responses).
        baseline = page.evaluate("""() => {
            const nodes = document.querySelectorAll('div.prose, div[class*="leading-relaxed"]');
            return Array.from(nodes).map(n => (n.innerText || "").trim()).filter(Boolean);
        }""")
        baseline_set = set(baseline)
        baseline_count = len(baseline)

        textarea.fill(prompt)
        time.sleep(0.1)
        textarea.press("Enter")

        start_time = time.time()
        last_text = ""
        stable_cycles = 0

        while time.time() - start_time < 90:
            time.sleep(0.5)

            # The Stop control carries aria-label="Stop" and has EMPTY innerText, so
            # :has-text('Stop') never matches it. Read the attribute instead.
            is_stop_visible = False
            try:
                stop_btn = page.locator('button[aria-label="Stop"]')
                is_stop_visible = stop_btn.count() > 0 and stop_btn.first.is_visible()
            except Exception:
                pass

            latest_info = page.evaluate("""() => {
                const nodes = document.querySelectorAll('div.prose, div[class*="leading-relaxed"]');
                if (!nodes || nodes.length === 0) return { count: 0, text: "" };
                const lastNode = nodes[nodes.length - 1];
                return { count: nodes.length, text: (lastNode.innerText || "").trim() };
            }""")

            cur_count = latest_info.get("count", 0)
            cur_text = latest_info.get("text", "")

            # Accept only content that is genuinely NEW relative to the baseline.
            is_new = bool(cur_text) and cur_count > baseline_count and cur_text not in baseline_set

            if is_new:
                if cur_text == last_text:
                    stable_cycles += 1
                    finished = (not is_stop_visible) or stable_cycles >= 10
                    if finished and stable_cycles >= 2:
                        logger.info(f"Response extracted: {len(cur_text)} chars in {time.time() - start_time:.2f}s")
                        return cur_text
                else:
                    stable_cycles = 0
                    last_text = cur_text
            else:
                # Reset so a stale pre-prompt value can never accumulate stability.
                stable_cycles = 0
                last_text = ""

        raise Exception("Muse.ai did not return response within timeout.")

    def generate(self, prompt, timeout=120):
        res_q = queue.Queue()
        self.req_queue.put(("generate", prompt, res_q))
        status, val = res_q.get(timeout=timeout)
        if status == "ok":
            return val
        else:
            raise val

    def reload_cookies(self, timeout=30):
        res_q = queue.Queue()
        self.req_queue.put(("reload", None, res_q))
        status, val = res_q.get(timeout=timeout)
        if status == "ok":
            return val
        else:
            raise val

    def reset_sessions(self, timeout=30):
        res_q = queue.Queue()
        self.req_queue.put(("reset", None, res_q))
        status, val = res_q.get(timeout=timeout)
        if status == "ok":
            return val
        else:
            raise val

    def get_status(self, timeout=10):
        res_q = queue.Queue()
        self.req_queue.put(("status", None, res_q))
        status, val = res_q.get(timeout=timeout)
        if status == "ok":
            return val
        else:
            return {"error": str(val)}


worker = MultiAccountBrowserWorker(COOKIES_FILE)


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class MuseHTTPHandler(BaseHTTPRequestHandler):
    def _send_cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")

    def do_OPTIONS(self):
        self.send_response(204)
        self._send_cors()
        self.end_headers()

    def do_GET(self):
        if self.path in ["/v1/models", "/models"]:
            models = {
                "object": "list",
                "data": [
                    {
                        "id": m,
                        "object": "model",
                        "created": 1700000000,
                        "owned_by": "meta"
                    }
                    for m in SUPPORTED_MODELS
                ]
            }
            res_bytes = json.dumps(models).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self._send_cors()
            self.send_header("Content-Length", str(len(res_bytes)))
            self.end_headers()
            self.wfile.write(res_bytes)
        elif self.path in ["/", "/health", "/v1", "/v1/health"]:
            status_data = worker.get_status()
            res_payload = {
                "status": "ok",
                "provider": "muse-spark-1.3",
                "models": SUPPORTED_MODELS,
                "pool": status_data
            }
            res_bytes = json.dumps(res_payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self._send_cors()
            self.send_header("Content-Length", str(len(res_bytes)))
            self.end_headers()
            self.wfile.write(res_bytes)
        elif self.path in ["/reload", "/v1/reload"]:
            try:
                count = worker.reload_cookies()
                res_payload = {"status": "ok", "active_accounts": count}
                self.send_response(200)
            except Exception as e:
                res_payload = {"status": "error", "message": str(e)}
                self.send_response(500)
            res_bytes = json.dumps(res_payload).encode("utf-8")
            self.send_header("Content-Type", "application/json")
            self._send_cors()
            self.send_header("Content-Length", str(len(res_bytes)))
            self.end_headers()
            self.wfile.write(res_bytes)
        elif self.path in ["/reset", "/v1/reset"]:
            try:
                count = worker.reset_sessions()
                res_payload = {"status": "ok", "reset_accounts": count}
                self.send_response(200)
            except Exception as e:
                res_payload = {"status": "error", "message": str(e)}
                self.send_response(500)
            res_bytes = json.dumps(res_payload).encode("utf-8")
            self.send_header("Content-Type", "application/json")
            self._send_cors()
            self.send_header("Content-Length", str(len(res_bytes)))
            self.end_headers()
            self.wfile.write(res_bytes)
        else:
            self.send_response(404)
            self._send_cors()
            self.end_headers()
            self.wfile.write(b'{"error":"Not Found"}')

    def do_POST(self):
        if self.path in ["/reload", "/v1/reload"]:
            try:
                count = worker.reload_cookies()
                res_payload = {"status": "ok", "active_accounts": count}
                self.send_response(200)
            except Exception as e:
                res_payload = {"status": "error", "message": str(e)}
                self.send_response(500)
            res_bytes = json.dumps(res_payload).encode("utf-8")
            self.send_header("Content-Type", "application/json")
            self._send_cors()
            self.send_header("Content-Length", str(len(res_bytes)))
            self.end_headers()
            self.wfile.write(res_bytes)
            return

        if self.path in ["/reset", "/v1/reset"]:
            try:
                count = worker.reset_sessions()
                res_payload = {"status": "ok", "reset_accounts": count}
                self.send_response(200)
            except Exception as e:
                res_payload = {"status": "error", "message": str(e)}
                self.send_response(500)
            res_bytes = json.dumps(res_payload).encode("utf-8")
            self.send_header("Content-Type", "application/json")
            self._send_cors()
            self.send_header("Content-Length", str(len(res_bytes)))
            self.end_headers()
            self.wfile.write(res_bytes)
            return

        if not (self.path.startswith("/v1/chat/completions") or self.path.startswith("/chat/completions")):
            self.send_response(404)
            self._send_cors()
            self.end_headers()
            self.wfile.write(b'{"error":"Not Found"}')
            return

        content_length = int(self.headers.get("Content-Length", 0))
        post_data = self.rfile.read(content_length)

        try:
            req_json = json.loads(post_data.decode("utf-8"))
        except Exception:
            self.send_response(400)
            self._send_cors()
            self.end_headers()
            self.wfile.write(b'{"error":"Invalid JSON"}')
            return

        messages = req_json.get("messages", [])
        model = req_json.get("model", "muse-spark-1.3")
        stream = req_json.get("stream", False)

        prompt_parts = []
        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if isinstance(content, list):
                text_items = [p.get("text", "") for p in content if isinstance(p, dict) and "text" in p]
                content = "\n".join(text_items)

            if role == "system":
                prompt_parts.append(f"[System Instruction]\n{content}")
            elif role == "assistant":
                prompt_parts.append(f"Assistant: {content}")
            else:
                prompt_parts.append(f"{content}")

        if len(prompt_parts) == 1:
            full_prompt = prompt_parts[0]
        else:
            full_prompt = "\n\n".join(prompt_parts)

        try:
            reply, acc_idx = worker.generate(full_prompt)
        except Exception as e:
            self.send_response(500)
            self._send_cors()
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode("utf-8"))
            return

        resp_id = "chatcmpl-" + str(uuid.uuid4()).replace("-", "")[:24]
        created = int(time.time())

        if stream:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.send_header("X-Account-Index", str(acc_idx))
            self._send_cors()
            self.end_headers()

            chunk1 = {
                "id": resp_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "delta": {"role": "assistant", "content": ""},
                        "finish_reason": None
                    }
                ]
            }
            body1 = "data: " + json.dumps(chunk1) + "\n\n"
            self.wfile.write(body1.encode("utf-8"))

            chunk2 = {
                "id": resp_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "delta": {"content": reply},
                        "finish_reason": "stop"
                    }
                ]
            }
            body2 = "data: " + json.dumps(chunk2) + "\n\n"
            self.wfile.write(body2.encode("utf-8"))
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            self.close_connection = True
        else:
            res_obj = {
                "id": resp_id,
                "object": "chat.completion",
                "created": created,
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": reply
                        },
                        "finish_reason": "stop"
                    }
                ],
                "usage": {
                    "prompt_tokens": len(full_prompt) // 4,
                    "completion_tokens": len(reply) // 4,
                    "total_tokens": (len(full_prompt) + len(reply)) // 4
                }
            }
            res_bytes = json.dumps(res_obj).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("X-Account-Index", str(acc_idx))
            self._send_cors()
            self.send_header("Content-Length", str(len(res_bytes)))
            self.end_headers()
            self.wfile.write(res_bytes)


def main():
    logger.info(f"Initializing Multi-Account Browser Worker from {COOKIES_FILE}...")
    worker.start()
    worker.ready_event.wait(timeout=60)

    logger.info(f"Starting Multi-Account Muse API Server on http://{HOST}:{PORT}/v1 ...")
    server = ThreadedHTTPServer((HOST, PORT), MuseHTTPHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        server.server_close()
        worker.req_queue.put(None)


if __name__ == "__main__":
    main()
