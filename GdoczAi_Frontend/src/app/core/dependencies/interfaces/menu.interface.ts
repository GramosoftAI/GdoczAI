export interface IMENU {
    code: string,
    screen_name: string,
    path: string,
    isShow: boolean
    icon?: string,
    subMenu?: ISUB_MENU[],
    type?: 'NR' | 'RE' |'LI'  // => NR = Normal, RE = routes without list, LI = List routes
}

export interface ISUB_MENU {
    code: string,
    screen_name: string,
    path: string,
    icon?: string,
    isShow: boolean,
    subMenu?: ISUB_MENU[]
}
