import http from "k6/http";
import { check, sleep } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://app:8080";

export const options = {
  scenarios: {
    products_rps_test: {
      executor: "ramping-arrival-rate",
      startRate: 100,
      timeUnit: "1s",
      preAllocatedVUs: 200,
      maxVUs: 2000,
      stages: [
        { target: 300, duration: "1m" },
        { target: 600, duration: "2m" },
        { target: 1000, duration: "3m" },
        { target: 1000, duration: "2m" }
      ]
    }
  },
  thresholds: {
    http_req_duration: ["p(95)<200"],
    http_req_failed: ["rate<0.01"]
  }
};

export default function () {
  const res = http.get(`${BASE_URL}/products`);
  check(res, {
    "status is 200": (r) => r.status === 200
  });
  sleep(0.1);
}
