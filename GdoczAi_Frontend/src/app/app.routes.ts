import { Routes } from '@angular/router';
import { AuthGuard } from './core/dependencies/services/auth.guard';

export const routes: Routes = [
  { path: '', redirectTo: 'auth', pathMatch: 'full' },
  { path: 'auth', loadChildren: () => import('./core/auth/auth-module').then(mod => mod.AuthModule) },
  { path: 'main', canActivate: [AuthGuard], loadChildren: () => import('./core/main/component/component-module').then(mod => mod.ComponentModule) },
  { path: 'shared', loadChildren: () => import('./core/main/shared/shared-module').then(mod => mod.SharedModule) }
];
