import { IMENU } from "../../dependencies/interfaces/menu.interface"

export const MENU: IMENU[] = [
    // {
    //     screen_name: 'Dashboard',
    //     path: '/main/dashboard',
    //     icon: '/assets/icons/dashboard.png',
    //     code: '',
    //     isShow: true
    // },
    {
        screen_name: 'Playground',
        path: '/main/playground',
        icon: '/assets/icons/playground.png',
        code: '',
        isShow: true
    },
    {
        screen_name: 'Usage',
        path: '/main/usage',
        icon: '/assets/icons/usage.png',
        code: '',
        isShow: true,
    },
    // {
    //     screen_name: 'Suscription',
    //     path: '/main/subscription',
    //     icon: '/assets/icons/subscription.png',
    //     code: '',
    //     isShow: true,
    // },
    {
        screen_name: 'Document Types',
        path: '/main/docType',
        icon: '/assets/icons/document.png',
        code: '',
        isShow: true,
    },
    {
        screen_name: 'Prompt Templates',
        path: '',
        icon: '/assets/icons/prompt.png',
        code: '',
        isShow: true,
        subMenu: [
          {
            screen_name: 'PreDefined Schema',
            path: '/main/business-logic/list',
            icon: '/assets/icons/prompt.png',
            code: '',
            isShow: true,
          },
          {
            screen_name: 'Schema Mapping',
            path: '/main/prompt-template/list',
            icon: '/assets/icons/prompt.png',
            code: '',
            isShow: true,
          }
        ]
    },
    {
        screen_name: 'Webhooks',
        path: '/main/webhook',
        icon: '/assets/icons/webhook.png',
        code: '',
        isShow: true,
    },
    {
        screen_name: 'API Keys',
        path: '/main/apiKeys',
        icon: '/assets/icons/api-key.png',
        code: '',
        isShow: true,
    },
    {
        screen_name: 'Connect',
        path: '/main/connectors/list',
        icon: '/assets/icons/webhook.png',
        code: '',
        isShow: true,
    },
]
