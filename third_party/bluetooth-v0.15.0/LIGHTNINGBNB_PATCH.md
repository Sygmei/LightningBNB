# LightningBNB patch

This directory is a source copy of `tinygo.org/x/bluetooth` v0.15.0. The main
module retains the v0.15.0 requirement and replaces it with this reviewed copy
to work around a Windows peripheral limitation.

LightningBNB adds `Service.ServiceData` and passes it to WinRT's
`GattServiceProviderAdvertisingParameters.ServiceData`. This attaches the
LightningBNB discovery marker to the same connectable GATT service
advertisement. The upstream generic Windows advertisement publisher cannot be
used for this purpose because that is a separate, non-connectable
advertisement.

The patch also retains the WinRT service-provider object for the service's
lifetime and waits for its asynchronous advertisement status. Startup fails
with an actionable error when Windows aborts advertising or omits the required
service data.

Remove this patch when upstream exposes service data on connectable Windows
GATT advertisements.
