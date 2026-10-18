# Cost calculation and billing convention

> **All prices in this project are fictional and illustrative.** They do not represent any real provider's
> pricing, and the data is synthetic. Results show that the pipeline works, not what LLMs cost.

## Formula

```
input_cost           = input_tokens  * input_price_per_million_tokens  / 1,000,000
output_cost          = output_tokens * output_price_per_million_tokens / 1,000,000
total_estimated_cost = input_cost + output_cost
```

- Prices are `DECIMAL(12,6)`; every cost column is `DECIMAL(18,8)` (no binary floating point).
- `input_cost` and `output_cost` are each rounded (half-up) to 8 decimals, and the total is their sum, so
  `total = input + output` holds exactly. At the sample prices an individual cost is exact at 8 decimals
  (1 token at 0.15/M = 0.00000015).
- The price used is the row of `silver_model_pricing` whose `[effective_from, effective_to]` (inclusive;
  `effective_to` NULL = open-ended) contains the request's **event date** (`request_timestamp` date) for the
  same `provider` + `model`. Pricing validation rejects overlapping ranges, so at most one row can match.

## Billing statuses

| `billing_status` | When | Cost columns |
|---|---|---|
| `priced` | status ≠ `rate_limited` and a price matches | calculated |
| `unpriced` | status ≠ `rate_limited` and **no** price matches (unknown model, or date outside every range) | **NULL** (never 0) |
| `not_billed` | status = `rate_limited` | explicit `0` |

## Synthetic billing convention (an assumption, not how any real provider bills)

| Case | Treatment |
|---|---|
| Successful request | Billed on recorded input and output tokens. |
| `error` / `timeout` | Billed on the recorded tokens (failed requests still consumed capacity). Output is often 0 in the generated data. |
| `rate_limited` | Rejected before processing: **not billed**. |
| Retries (`retry_count`) | The token counts on a row already cover every attempt of that request, so there is no retry multiplier. "Cost of requests with retries" is the whole cost of those requests, not an isolated surcharge. |
| Cache hits (`cache_hit`) | Billed like any other request; the flag is used for hit-rate reporting only. |
| Duplicate ingestion | **Never** billed twice. Duplicates share a `request_id` and collapse to one record in Silver. |

## Duplicates vs retries

- A *duplicate ingestion* is the same request delivered more than once. It is resolved in Silver (see
  `data_quality_rules.md`) and counted once.
- A *retry* is part of the request's own history (`retry_count`). If a retried attempt were logged as a
  separate request with a **different** `request_id`, it is a distinct usage event and is kept.

## Limitations

- `unpriced` requests are excluded from every cost total, so spend and budget utilization are understated by
  the unpriced share (reported as `unpriced_requests`, and in the pricing-coverage metric).
- Cost per successful request divides *all* priced spend of a model by its successful requests; it includes the
  cost of failed requests by design, and is NULL when any request in the group is unpriced.
