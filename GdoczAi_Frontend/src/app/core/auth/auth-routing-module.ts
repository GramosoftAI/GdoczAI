import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

const routes: Routes = [
  { path: '', redirectTo: 'sign_in', pathMatch: 'full' },
  { path: 'sign_in', loadComponent: () => import('./login/login').then(m => m.Login) },
  { path: 'reset_password', loadComponent: () => import('./reset-password/reset-password').then(m => m.ResetPassword) },
  { path: 'demo', loadComponent: () => import('./demo-page/demo-page').then(m => m.DemoPage) },
  { path: 'contact-us', loadComponent: () => import('./contact-us/contact-us').then(m => m.ContactUs) }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class AuthRoutingModule { }
