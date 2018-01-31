# billing

The billing service is responsible for managing stripe subscriptions and updating the:

 * `CustomerId`
 * `CurrentPlan`

fields for the user in the core dotmesh-server.

#### initial billing status

The frontend will call `http://billing/api/v1/status` - this returns info about the current user payment status.

The answer will include:

 * `CurrentPlan` - string that represents the users currently active plan
 * `CustomerId` - stripe id associated with user
 * `PaymentCards` - a list of the cards stripe knows for the CustomerId
 * `SubscriptionStatus` - the current status of the subscription
 * `Invoices` - a list of the previous payments mades

#### billing page

This data will be used to generate the payment page that has the following elements:

 * `Actions` - any actions the user needs to take listed here
   * `Update Payment Info` - in the case of a failed sub
 * `CurrentPlan` - display what plan the user is currently on including details of the plan
 * `ChangePlans` - a list of potential plan changes the user could make - either upgrades or downgrades
 * `PaymentCards` - a list of the cards on record - add/edit/remove/make default
 * `Invoices` - a list of the previous payments made