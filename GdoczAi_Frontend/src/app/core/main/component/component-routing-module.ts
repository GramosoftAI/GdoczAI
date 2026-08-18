import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { Container } from '../container/container';
import { PromtTemplate } from './promt-template/promt-template';
import { PromptTemplateForm } from './promt-template/prompt-template-form/prompt-template-form';
import { PromptTemplateList } from './promt-template/prompt-template-list/prompt-template-list';
import { BusinessLogic } from './business-logic/business-logic';
import { BusinessLogicForm } from './business-logic/business-logic-form/business-logic-form';
import { BusinessLogicList } from './business-logic/business-logic-list/business-logic-list';

const routes: Routes = [
  {path: '', component: Container,
    children: [
      {path: 'dashboard', loadComponent: () => import('./dashboard/dashboard').then(m => m.Dashboard) },
      {path: 'playground', loadComponent: () => import('./playground/playground').then(m => m.Playground)},
      {path: 'playground/:id', loadComponent: () => import('./playground/playground').then(m => m.Playground)},
      {path: 'usage', loadComponent: () => import('./usage/usage').then(m => m.Usage)},
      {path: 'docType', loadComponent: () => import('./doc-type/doc-type').then(m => m.DocType)},

      // PROMT_TEMPLATE
      { path: 'prompt-template', component: PromtTemplate,
        children: [
          { path: 'create', component: PromptTemplateForm},
          { path: 'list', component: PromptTemplateList},
          { path: 'edit/:id', component: PromptTemplateForm},
          { path: 'details/:id', component: PromptTemplateForm}
        ]
      },

      // Business Logic
      { path: 'business-logic', component: BusinessLogic,
        children: [
          { path: 'create', component: BusinessLogicForm},
          { path: 'list', component: BusinessLogicList},
          { path: 'edit/:id', component: BusinessLogicForm},
          { path: 'details/:id', component: BusinessLogicForm}
        ]
      },
      // {path: 'prompt-template', loadComponent: () => import('./promt-template/promt-template').then(m => m.PromtTemplate)},
      // {path: 'prompt-template/:id', loadComponent: () => import('./promt-template/promt-template').then(m => m.PromtTemplate)},
      {path: 'webhook', loadComponent: () => import('./webhook/webhook').then(m => m.Webhook)},
      {path: 'apiKeys', loadComponent: () => import('./api-keys/api-keys').then(m => m.ApiKeys)},

      // Connectors
      {path: 'connectors/list', loadComponent: () => import('./connector/connector-list/connector-list').then(m => m.ConnectorList)},
      {path: 'connectors/create', loadComponent: () => import('./connector/connector-form/connector-form').then(m => m.ConnectorForm)},
      {path: 'connectors/edit/:type', loadComponent: () => import('./connector/connector-form/connector-form').then(m => m.ConnectorForm)},

    ]
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class ComponentRoutingModule { }
