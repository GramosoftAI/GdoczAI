import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { TermsUser } from './terms-user/terms-user';
import { PrivacyPolicy } from './privacy-policy/privacy-policy';

const routes: Routes = [
  {path: 'terms-user', loadComponent: () => import('./terms-user/terms-user').then(m => m.TermsUser)},
  {path: 'privacy-policy', loadComponent: () => import('./privacy-policy/privacy-policy').then(m => m.PrivacyPolicy)},
  {path: 'data-use-policy', loadComponent: () => import('./data-use-policy/data-use-policy').then(m => m.DataUsePolicy)}
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class SharedRoutingModule { }
