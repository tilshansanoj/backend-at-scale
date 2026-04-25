import http from "k6/http";
import { check, sleep } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://app:8080";

export const options = {
  scenarios: {
    mixed: {
      executor: "ramping-arrival-rate",
      startRate: 250,
      timeUnit: "1s",
      preAllocatedVUs: 500,
      maxVUs: 6000,
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
      headers: { "Content-Type": "application/json" }
    });
    check(res, { "POST /products 201": (r) => r.status === 201 });
  } else {
    const res = http.get(`${BASE_URL}/products`);
    check(res, { "GET /products 200": (r) => r.status === 200 });
  }
  sleep(0.02);
}
