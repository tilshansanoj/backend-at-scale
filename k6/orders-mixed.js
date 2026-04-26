import http from "k6/http";
import { check, sleep } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://app:8080";

// Whitelist system tags (k6 ≤0.53 expects `systemTags` as string[], not `{ exclude }`). Omit `url` so dynamic paths do not explode cardinality.
const systemTagsNoURL = [
  "check",
  "error",
  "error_code",
  "expected_response",
  "group",
  "method",
  "name",
  "proto",
  "scenario",
  "service",
  "status",
  "subproto",
  "tls_version"
];

export const options = {
  systemTags: systemTagsNoURL,
  scenarios: {
    mixed: {
      executor: "ramping-arrival-rate",
      startRate: 150,
      timeUnit: "1s",
      preAllocatedVUs: 400,
      maxVUs: 10000,
      stages: [
        { target: 500, duration: "45s" },
        { target: 1400, duration: "1m" },
        { target: 2200, duration: "1m" }
      ]
    }
  },
  thresholds: {
    // Without responseCallback, k6 counts every 4xx as failed; GET /orders/:id often returns 404 until the async consumer has written the row.
    http_req_failed: ["rate<0.06"],
    http_req_duration: ["p(99)<8000"]
  }
};

export default function () {
  const body = JSON.stringify({
    product_id: 1,
    quantity: 1 + Math.floor(Math.random() * 5)
  });
  const res = http.post(`${BASE_URL}/orders`, body, {
    headers: { "Content-Type": "application/json" },
    tags: { name: "POST /orders" },
    responseCallback: http.expectedStatuses(202)
  });
  check(res, { "POST /orders 202": (r) => r.status === 202 });
  if (res.status !== 202) {
    sleep(0.02);
    return;
  }

  let requestId = "";
  try {
    const j = JSON.parse(res.body);
    requestId = j.request_id || "";
  } catch (_) {
    sleep(0.02);
    return;
  }

  if (requestId && Math.random() < 0.72) {
    const g = http.get(`${BASE_URL}/orders/${encodeURIComponent(requestId)}`, {
      tags: { name: "GET /orders/:request_id" },
      responseCallback: http.expectedStatuses(200, 404)
    });
    check(g, {
      "GET /orders 200 or 404": (r) => r.status === 200 || r.status === 404
    });
  }
  sleep(0.02);
}
