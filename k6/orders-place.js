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
    orders_place: {
      executor: "ramping-arrival-rate",
      startRate: 100,
      timeUnit: "1s",
      preAllocatedVUs: 200,
      maxVUs: 8000,
      stages: [
        { target: 400, duration: "45s" },
        { target: 1200, duration: "1m" },
        { target: 2000, duration: "1m" }
      ]
    }
  },
  thresholds: {
    http_req_failed: ["rate<0.05"],
    http_req_duration: ["p(99)<5000"]
  }
};

export default function () {
  const body = JSON.stringify({
    product_id: 1,
    quantity: 1 + Math.floor(Math.random() * 3)
  });
  const res = http.post(`${BASE_URL}/orders`, body, {
    headers: { "Content-Type": "application/json" },
    tags: { name: "POST /orders" },
    responseCallback: http.expectedStatuses(202)
  });
  check(res, { "POST /orders 202": (r) => r.status === 202 });
  sleep(0.02);
}
