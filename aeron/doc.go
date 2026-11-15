// Package aeron implements the Aeron transport client.
//
// Connect maps the media driver CnC file and returns a Client. The Client adds
// publications and subscriptions through an external C or Java media driver.
//
// Connect starts a conductor goroutine unless Config.UseConductorInvoker is
// true. That goroutine runs the Client.DoWork duty cycle. In invoker mode the
// caller runs DoWork.
package aeron
