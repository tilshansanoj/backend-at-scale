import http from "k6/http";
import { check, sleep } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://app:8080";

// Spike: steady traffic, sudden sharp ramp, hold, then recover.
export const options = {
  scenarios: {
    spike: {
      executor: "ramping-arrival-rate",
      startRate: 100,
      timeUnit: "1s",
      preAllocatedVUs: 400,
      maxVUs: 15000,
      stages: [
        { target: 400, duration: "2m" },
        { target: 4500, duration: "15s" },
        { target: 4500, duration: "1m" },
        { target: 400, duration: "2m" }
      ]
    }
  },
  thresholds: {
    http_req_failed: ["rate<0.10"]
  }
};

export default function () {
  if (Math.random() < 0.12) {
    const body = JSON.stringify({
      name: `spike-${__VU}-${Date.now()}`,
      price: Math.round((10 + Math.random() * 500) * 100) / 100
    });
    const res = http.post(`${BASE_URL}/products`, body, {
      headers: { "Content-Type": "application/json" }
    });
    check(res, { "POST /products 202": (r) => r.status === 202 });
  } else {
    const res = http.get(`${BASE_URL}/products`);
    check(res, { "GET /products 200": (r) => r.status === 200 });
  }
  sleep(0.02);
}
