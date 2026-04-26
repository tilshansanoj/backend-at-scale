import http from "k6/http";
import { check, sleep } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://app:8080";

// Stress: step up arrival rate in plateaus, hold at high load, then cool down.
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
    stress: {
      executor: "ramping-arrival-rate",
      startRate: 80,
      timeUnit: "1s",
      preAllocatedVUs: 600,
      maxVUs: 16000,
      stages: [
        { target: 400, duration: "1m" },
        { target: 900, duration: "2m" },
        { target: 1600, duration: "2m" },
        { target: 2400, duration: "2m" },
        { target: 3200, duration: "2m" },
        { target: 4000, duration: "5m" },
        { target: 500, duration: "2m" }
      ]
    }
  },
  thresholds: {
    http_req_failed: ["rate<0.15"]
  }
};

export default function () {
  if (Math.random() < 0.12) {
    const body = JSON.stringify({
      name: `stress-${__VU}-${Date.now()}`,
      price: Math.round((10 + Math.random() * 500) * 100) / 100
    });
    const res = http.post(`${BASE_URL}/products`, body, {
      headers: { "Content-Type": "application/json" },
      tags: { name: "POST /products" }
    });
    check(res, { "POST /products 202": (r) => r.status === 202 });
  } else {
    const res = http.get(`${BASE_URL}/products`, { tags: { name: "GET /products" } });
    check(res, { "GET /products 200": (r) => r.status === 200 });
  }
  sleep(0.02);
}
