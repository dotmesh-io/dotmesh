# stripe plans and billing

 * [stripe subscription lifecycle](https://stripe.com/docs/subscriptions/lifecycle)
 * [sending emails for failed payments](https://stripe.com/docs/recipes/sending-emails-for-failed-payments)
 * [webhook event cheatsheet](https://www.masteringmodernpayments.com/stripe-webhook-event-cheatsheet)
 * [testing failed payments](https://stripe.com/docs/recipes/sending-emails-for-failed-payments#testing)
 * [card management api](https://stripe.com/blog/multiple-cards)
 * [webhook cheatsheet](https://www.masteringmodernpayments.com/stripe-webhook-event-cheatsheet)

## intro

This is how we use Stripe to do the billing section.

## plans

Currently passed to dotmesh-server via the `config.yaml`

```yaml
Plans:
  - 
    Id: free
    Name: Free
    TotalSize: 100M
    Transfer: 1G
    VolumeCount: 5
    PriceUSD: 0
  - 
    Id: developer
    Name: "Developer Plan"
    TotalSize: 1G
    Transfer: 10G
    VolumeCount: 10
    PriceUSD: 2500
  - 
    Id: team
    Name: team
    TotalSize: 10G
    Transfer: 100G
    VolumeCount: 100
    PriceUSD: 10000

```

The user struct:

```go
type User struct {
    Id          string
    Name        string
    Email       string
    ApiKey      string
    CustomerId  string
    CurrentPlan string
}
```

The `CustomerId` points to the Stripe id for the user - we should be able to display all their billing history and everything else using this.

The `CurrentPlan` is our current determination of their status - this is worked out as a result of each incoming WebHook.

## webhooks

The server should be setup to receive webhooks on `/stripe` - read [dev-commands.md](dev-commands.md) to get this setup locally.

[here](https://stripe.com/docs/api#event_types) is a list of the Stripe event types

## first time payment

Example webhooks for a first time payment in the order they came in - all with test mode:

 1. invoice.created
 2. customer.created
 3. customer.subscription.created
 4. invoice.payment_succeeded
 5. charge.succeeded
 6. customer.updated
 7. customer.source.created

**invoice.created**

```json
{
  "id": "evt_1B3Z3RKk0JfbxScKA4c1V3se",
  "object": "event",
  "api_version": "2017-08-15",
  "created": 1505778813,
  "data": {
    "object": {
      "id": "in_1B3Z3RKk0JfbxScKssG9gGjj",
      "object": "invoice",
      "amount_due": 2500,
      "application_fee": null,
      "attempt_count": 0,
      "attempted": true,
      "billing": "charge_automatically",
      "charge": "ch_1B3Z3RKk0JfbxScKxJiY65Ue",
      "closed": true,
      "currency": "usd",
      "customer": "cus_BQPtcL5VMHCNco",
      "date": 1505778813,
      "description": null,
      "discount": null,
      "ending_balance": 0,
      "forgiven": false,
      "lines": {
        "object": "list",
        "data": [
          {
            "id": "sub_BQPt31J2fumH72",
            "object": "line_item",
            "amount": 2500,
            "currency": "usd",
            "description": null,
            "discountable": true,
            "livemode": false,
            "metadata": {
            },
            "period": {
              "start": 1505778813,
              "end": 1508370813
            },
            "plan": {
              "id": "developer",
              "object": "plan",
              "amount": 2500,
              "created": 1504892291,
              "currency": "usd",
              "interval": "month",
              "interval_count": 1,
              "livemode": false,
              "metadata": {
              },
              "name": "Developer",
              "statement_descriptor": "Dotmesh",
              "trial_period_days": null
            },
            "proration": false,
            "quantity": 1,
            "subscription": null,
            "subscription_item": "si_1B3Z3RKk0JfbxScKFG1tCIVd",
            "type": "subscription"
          }
        ],
        "has_more": false,
        "total_count": 1,
        "url": "/v1/invoices/in_1B3Z3RKk0JfbxScKssG9gGjj/lines"
      },
      "livemode": false,
      "metadata": {
      },
      "next_payment_attempt": null,
      "number": "fa7881caeb-0001",
      "paid": true,
      "period_end": 1505778813,
      "period_start": 1505778813,
      "receipt_number": null,
      "starting_balance": 0,
      "statement_descriptor": null,
      "subscription": "sub_BQPt31J2fumH72",
      "subtotal": 2500,
      "tax": null,
      "tax_percent": null,
      "total": 2500,
      "webhooks_delivered_at": null
    }
  },
  "livemode": false,
  "pending_webhooks": 1,
  "request": {
    "id": "req_LnCxrs5WldLUaK",
    "idempotency_key": null
  },
  "type": "invoice.created"
}
```

**customer.created**

```json
{
  "id": "evt_1B3Z3QKk0JfbxScKGUHgpBcX",
  "object": "event",
  "api_version": "2017-08-15",
  "created": 1505778812,
  "data": {
    "object": {
      "id": "cus_BQPtcL5VMHCNco",
      "object": "customer",
      "account_balance": 0,
      "created": 1505778812,
      "currency": null,
      "default_source": "card_1B3Z3NKk0JfbxScKvUiL0HlF",
      "delinquent": false,
      "description": "Customer for t@t.com",
      "discount": null,
      "email": null,
      "livemode": false,
      "metadata": {
      },
      "shipping": null,
      "sources": {
        "object": "list",
        "data": [
          {
            "id": "card_1B3Z3NKk0JfbxScKvUiL0HlF",
            "object": "card",
            "address_city": null,
            "address_country": null,
            "address_line1": null,
            "address_line1_check": null,
            "address_line2": null,
            "address_state": null,
            "address_zip": null,
            "address_zip_check": null,
            "brand": "Visa",
            "country": "US",
            "customer": "cus_BQPtcL5VMHCNco",
            "cvc_check": "pass",
            "dynamic_last4": null,
            "exp_month": 1,
            "exp_year": 2018,
            "fingerprint": "GmjUX4QzWwSp57bW",
            "funding": "credit",
            "last4": "4242",
            "metadata": {
            },
            "name": "t@t.com",
            "tokenization_method": null
          }
        ],
        "has_more": false,
        "total_count": 1,
        "url": "/v1/customers/cus_BQPtcL5VMHCNco/sources"
      },
      "subscriptions": {
        "object": "list",
        "data": [

        ],
        "has_more": false,
        "total_count": 0,
        "url": "/v1/customers/cus_BQPtcL5VMHCNco/subscriptions"
      }
    }
  },
  "livemode": false,
  "pending_webhooks": 1,
  "request": {
    "id": "req_YtP5n9f2hQqNqj",
    "idempotency_key": null
  },
  "type": "customer.created"
}
```

**customer.subscription.created**

```json
{
  "id": "evt_1B3Z3RKk0JfbxScKjHtjLU9l",
  "object": "event",
  "api_version": "2017-08-15",
  "created": 1505778813,
  "data": {
    "object": {
      "id": "sub_BQPt31J2fumH72",
      "object": "subscription",
      "application_fee_percent": null,
      "billing": "charge_automatically",
      "cancel_at_period_end": false,
      "canceled_at": null,
      "created": 1505778813,
      "current_period_end": 1508370813,
      "current_period_start": 1505778813,
      "customer": "cus_BQPtcL5VMHCNco",
      "discount": null,
      "ended_at": null,
      "items": {
        "object": "list",
        "data": [
          {
            "id": "si_1B3Z3RKk0JfbxScKFG1tCIVd",
            "object": "subscription_item",
            "created": 1505778813,
            "metadata": {
            },
            "plan": {
              "id": "developer",
              "object": "plan",
              "amount": 2500,
              "created": 1504892291,
              "currency": "usd",
              "interval": "month",
              "interval_count": 1,
              "livemode": false,
              "metadata": {
              },
              "name": "Developer",
              "statement_descriptor": "Dotmesh",
              "trial_period_days": null
            },
            "quantity": 1
          }
        ],
        "has_more": false,
        "total_count": 1,
        "url": "/v1/subscription_items?subscription=sub_BQPt31J2fumH72"
      },
      "livemode": false,
      "metadata": {
      },
      "plan": {
        "id": "developer",
        "object": "plan",
        "amount": 2500,
        "created": 1504892291,
        "currency": "usd",
        "interval": "month",
        "interval_count": 1,
        "livemode": false,
        "metadata": {
        },
        "name": "Developer",
        "statement_descriptor": "Dotmesh",
        "trial_period_days": null
      },
      "quantity": 1,
      "start": 1505778813,
      "status": "active",
      "tax_percent": null,
      "trial_end": null,
      "trial_start": null
    }
  },
  "livemode": false,
  "pending_webhooks": 1,
  "request": {
    "id": "req_LnCxrs5WldLUaK",
    "idempotency_key": null
  },
  "type": "customer.subscription.created"
}
```

**invoice.payment_succeeded**

```json
{
  "id": "evt_1B3Z3RKk0JfbxScKASmMbfQr",
  "object": "event",
  "api_version": "2017-08-15",
  "created": 1505778813,
  "data": {
    "object": {
      "id": "in_1B3Z3RKk0JfbxScKssG9gGjj",
      "object": "invoice",
      "amount_due": 2500,
      "application_fee": null,
      "attempt_count": 0,
      "attempted": true,
      "billing": "charge_automatically",
      "charge": "ch_1B3Z3RKk0JfbxScKxJiY65Ue",
      "closed": true,
      "currency": "usd",
      "customer": "cus_BQPtcL5VMHCNco",
      "date": 1505778813,
      "description": null,
      "discount": null,
      "ending_balance": 0,
      "forgiven": false,
      "lines": {
        "object": "list",
        "data": [
          {
            "id": "sub_BQPt31J2fumH72",
            "object": "line_item",
            "amount": 2500,
            "currency": "usd",
            "description": null,
            "discountable": true,
            "livemode": false,
            "metadata": {
            },
            "period": {
              "start": 1505778813,
              "end": 1508370813
            },
            "plan": {
              "id": "developer",
              "object": "plan",
              "amount": 2500,
              "created": 1504892291,
              "currency": "usd",
              "interval": "month",
              "interval_count": 1,
              "livemode": false,
              "metadata": {
              },
              "name": "Developer",
              "statement_descriptor": "Dotmesh",
              "trial_period_days": null
            },
            "proration": false,
            "quantity": 1,
            "subscription": null,
            "subscription_item": "si_1B3Z3RKk0JfbxScKFG1tCIVd",
            "type": "subscription"
          }
        ],
        "has_more": false,
        "total_count": 1,
        "url": "/v1/invoices/in_1B3Z3RKk0JfbxScKssG9gGjj/lines"
      },
      "livemode": false,
      "metadata": {
      },
      "next_payment_attempt": null,
      "number": "fa7881caeb-0001",
      "paid": true,
      "period_end": 1505778813,
      "period_start": 1505778813,
      "receipt_number": null,
      "starting_balance": 0,
      "statement_descriptor": null,
      "subscription": "sub_BQPt31J2fumH72",
      "subtotal": 2500,
      "tax": null,
      "tax_percent": null,
      "total": 2500,
      "webhooks_delivered_at": null
    }
  },
  "livemode": false,
  "pending_webhooks": 1,
  "request": {
    "id": "req_LnCxrs5WldLUaK",
    "idempotency_key": null
  },
  "type": "invoice.payment_succeeded"
}
```

**charge.succeeded**

```json
{
  "id": "evt_1B3Z3RKk0JfbxScKzuz93BYS",
  "object": "event",
  "api_version": "2017-08-15",
  "created": 1505778813,
  "data": {
    "object": {
      "id": "ch_1B3Z3RKk0JfbxScKxJiY65Ue",
      "object": "charge",
      "amount": 2500,
      "amount_refunded": 0,
      "application": null,
      "application_fee": null,
      "balance_transaction": "txn_1B3Z3RKk0JfbxScKkuzoB0PR",
      "captured": true,
      "created": 1505778813,
      "currency": "usd",
      "customer": "cus_BQPtcL5VMHCNco",
      "description": null,
      "destination": null,
      "dispute": null,
      "failure_code": null,
      "failure_message": null,
      "fraud_details": {
      },
      "invoice": "in_1B3Z3RKk0JfbxScKssG9gGjj",
      "livemode": false,
      "metadata": {
      },
      "on_behalf_of": null,
      "order": null,
      "outcome": {
        "network_status": "approved_by_network",
        "reason": null,
        "risk_level": "normal",
        "seller_message": "Payment complete.",
        "type": "authorized"
      },
      "paid": true,
      "receipt_email": null,
      "receipt_number": null,
      "refunded": false,
      "refunds": {
        "object": "list",
        "data": [

        ],
        "has_more": false,
        "total_count": 0,
        "url": "/v1/charges/ch_1B3Z3RKk0JfbxScKxJiY65Ue/refunds"
      },
      "review": null,
      "shipping": null,
      "source": {
        "id": "card_1B3Z3NKk0JfbxScKvUiL0HlF",
        "object": "card",
        "address_city": null,
        "address_country": null,
        "address_line1": null,
        "address_line1_check": null,
        "address_line2": null,
        "address_state": null,
        "address_zip": null,
        "address_zip_check": null,
        "brand": "Visa",
        "country": "US",
        "customer": "cus_BQPtcL5VMHCNco",
        "cvc_check": "pass",
        "dynamic_last4": null,
        "exp_month": 1,
        "exp_year": 2018,
        "fingerprint": "GmjUX4QzWwSp57bW",
        "funding": "credit",
        "last4": "4242",
        "metadata": {
        },
        "name": "t@t.com",
        "tokenization_method": null
      },
      "source_transfer": null,
      "statement_descriptor": "Dotmesh",
      "status": "succeeded",
      "transfer_group": null
    }
  },
  "livemode": false,
  "pending_webhooks": 1,
  "request": {
    "id": "req_LnCxrs5WldLUaK",
    "idempotency_key": null
  },
  "type": "charge.succeeded"
}
```

**customer.updated**

```json
{
  "id": "evt_1B3Z3RKk0JfbxScK3tO9zcL9",
  "object": "event",
  "api_version": "2017-08-15",
  "created": 1505778813,
  "data": {
    "object": {
      "id": "cus_BQPtcL5VMHCNco",
      "object": "customer",
      "account_balance": 0,
      "created": 1505778812,
      "currency": "usd",
      "default_source": "card_1B3Z3NKk0JfbxScKvUiL0HlF",
      "delinquent": false,
      "description": "Customer for t@t.com",
      "discount": null,
      "email": null,
      "livemode": false,
      "metadata": {
      },
      "shipping": null,
      "sources": {
        "object": "list",
        "data": [
          {
            "id": "card_1B3Z3NKk0JfbxScKvUiL0HlF",
            "object": "card",
            "address_city": null,
            "address_country": null,
            "address_line1": null,
            "address_line1_check": null,
            "address_line2": null,
            "address_state": null,
            "address_zip": null,
            "address_zip_check": null,
            "brand": "Visa",
            "country": "US",
            "customer": "cus_BQPtcL5VMHCNco",
            "cvc_check": "pass",
            "dynamic_last4": null,
            "exp_month": 1,
            "exp_year": 2018,
            "fingerprint": "GmjUX4QzWwSp57bW",
            "funding": "credit",
            "last4": "4242",
            "metadata": {
            },
            "name": "t@t.com",
            "tokenization_method": null
          }
        ],
        "has_more": false,
        "total_count": 1,
        "url": "/v1/customers/cus_BQPtcL5VMHCNco/sources"
      },
      "subscriptions": {
        "object": "list",
        "data": [
          {
            "id": "sub_BQPt31J2fumH72",
            "object": "subscription",
            "application_fee_percent": null,
            "billing": "charge_automatically",
            "cancel_at_period_end": false,
            "canceled_at": null,
            "created": 1505778813,
            "current_period_end": 1508370813,
            "current_period_start": 1505778813,
            "customer": "cus_BQPtcL5VMHCNco",
            "discount": null,
            "ended_at": null,
            "items": {
              "object": "list",
              "data": [
                {
                  "id": "si_1B3Z3RKk0JfbxScKFG1tCIVd",
                  "object": "subscription_item",
                  "created": 1505778813,
                  "metadata": {
                  },
                  "plan": {
                    "id": "developer",
                    "object": "plan",
                    "amount": 2500,
                    "created": 1504892291,
                    "currency": "usd",
                    "interval": "month",
                    "interval_count": 1,
                    "livemode": false,
                    "metadata": {
                    },
                    "name": "Developer",
                    "statement_descriptor": "Dotmesh",
                    "trial_period_days": null
                  },
                  "quantity": 1
                }
              ],
              "has_more": false,
              "total_count": 1,
              "url": "/v1/subscription_items?subscription=sub_BQPt31J2fumH72"
            },
            "livemode": false,
            "metadata": {
            },
            "plan": {
              "id": "developer",
              "object": "plan",
              "amount": 2500,
              "created": 1504892291,
              "currency": "usd",
              "interval": "month",
              "interval_count": 1,
              "livemode": false,
              "metadata": {
              },
              "name": "Developer",
              "statement_descriptor": "Dotmesh",
              "trial_period_days": null
            },
            "quantity": 1,
            "start": 1505778813,
            "status": "active",
            "tax_percent": null,
            "trial_end": null,
            "trial_start": null
          }
        ],
        "has_more": false,
        "total_count": 1,
        "url": "/v1/customers/cus_BQPtcL5VMHCNco/subscriptions"
      }
    },
    "previous_attributes": {
      "currency": null
    }
  },
  "livemode": false,
  "pending_webhooks": 1,
  "request": {
    "id": "req_LnCxrs5WldLUaK",
    "idempotency_key": null
  },
  "type": "customer.updated"
}
```

**customer.source.created**

```json
{
  "id": "evt_1B3Z3QKk0JfbxScKjKQ5fZxz",
  "object": "event",
  "api_version": "2017-08-15",
  "created": 1505778812,
  "data": {
    "object": {
      "id": "card_1B3Z3NKk0JfbxScKvUiL0HlF",
      "object": "card",
      "address_city": null,
      "address_country": null,
      "address_line1": null,
      "address_line1_check": null,
      "address_line2": null,
      "address_state": null,
      "address_zip": null,
      "address_zip_check": null,
      "brand": "Visa",
      "country": "US",
      "customer": "cus_BQPtcL5VMHCNco",
      "cvc_check": "pass",
      "dynamic_last4": null,
      "exp_month": 1,
      "exp_year": 2018,
      "fingerprint": "GmjUX4QzWwSp57bW",
      "funding": "credit",
      "last4": "4242",
      "metadata": {
      },
      "name": "t@t.com",
      "tokenization_method": null
    }
  },
  "livemode": false,
  "pending_webhooks": 1,
  "request": {
    "id": "req_YtP5n9f2hQqNqj",
    "idempotency_key": null
  },
  "type": "customer.source.created"
}

```
