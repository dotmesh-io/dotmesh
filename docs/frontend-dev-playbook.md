# frontend development playbook

Some common workflows for local frontend development with a single node.

This is useful if working on the frontend or auxillary services like billing.

## credentials.env

You will need a `credentials.env` file that looks like the following:

```
STRIPE_PUBLIC_KEY=pk_test_...
STRIPE_SECRET_KEY=sk_test_...
STRIPE_SIGNATURE_SECRET=whsec_...
MAILGUN_API_KEY=key-...
MAILGUN_DOMAIN=mail.dotmesh.io
SEGMENT_API_KEY=...
```

## Full local stack

First build components:

```bash
$ make build
```

if you want to activate analytics locally so you can debug:

```bash
$ export LOCAL_ANALYTICS=1
```

Boot the stack:

```bash
$ export LOCAL_BILLING=1
$ export LOCAL_COMMUNICATIONS=1
$ make reset
$ make cluster.start
$ make billing.dev
root$ node src/index.js
```

In a new terminal, start the communications service:

```bash
$ make communications.dev
root$ node src/index.js
```

If you want to actually send emails (**NOTE** make sure you enter an email address you can read and not poor `bob@bob.com`)

```bash
$ export ACTIVATED=1
$ make communications.dev
root$ node src/index.js
```


In a new terminal, start the frontend:

```bash
$ make frontend.dev
root$ yarn run watch
```

You can now open [http://localhost:8080](http://localhost:8080) in your browser

The URL [http://localhost:8088](http://localhost:8088/webhook) is hooked up to the billing service - you can use this in the Stripe [webhooks](https://dashboard.stripe.com/account/webhooks) console if you do some port fowarding.

Or you can use a localtunnel address which makes this automatic but is a big buggy (the connection will drop every 20 mins):

```
$ make billing.url
```

This stack is using the development Stripe keys - you can register and pay using the following test card:

```
number: 4242 4242 4242 4242
date: 01/20
cvc: 123
```

## Local testing

```bash
$ make frontend.test.build
$ make chromedriver.start
$ make gotty.start
$ make frontend.test
```

To run a single named test:

```bash
$ export TEST_NAME=specs/payment.js
$ make frontend.test
```

To run the frontend tests quicker (less good for viewable video):

```bash
$ export QUICK=1
$ make frontend.test
```

## Variables

#### LOCAL_BILLING

activate the billing service, this makes `dotmesh-server-inner` register the local billing server and use it.

#### LOCAL_COMMUNICATIONS

tells the billing service that we are using a local `dotmesh-communications` server - this will make the billing service contact the communications server when needed

#### ACTIVATE_COMMUNICATIONS

Use this to make the communications server actually send emails - disabled by default

