import http from "k6/http";
import { check, sleep } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://app:8080";

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
      startRate: 250,
      timeUnit: "1s",
      preAllocatedVUs: 500,
      maxVUs: 12000,
      stages: [
        { target: 800, duration: "1m" },
        { target: 1800, duration: "2m" },
        { target: 3000, duration: "2m" }
      ]
    }
  },
  thresholds: {
    http_req_failed: ["rate<0.05"]
  }
};

export default function () {
  if (Math.random() < 0.12) {
    const body = JSON.stringify({
      name: `load-${__VU}-${Date.now()}`,
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
