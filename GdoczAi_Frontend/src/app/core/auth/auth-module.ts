import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { AuthRoutingModule } from './auth-routing-module';
import { Login } from './login/login';
import { ForgotPassword } from './forgot-password/forgot-password';
import { ContactUs } from './contact-us/contact-us';


@NgModule({
  declarations: [],
  imports: [
    CommonModule,
    AuthRoutingModule,
    Login,
    ForgotPassword,
    ContactUs
  ]
})
export class AuthModule { }
