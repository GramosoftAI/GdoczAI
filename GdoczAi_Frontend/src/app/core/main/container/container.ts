import { Component, HostListener, inject, ViewChild } from '@angular/core';
import { IMENU } from '../../dependencies/interfaces/menu.interface';
import { MENU } from '.';
import { RouterModule } from '@angular/router';
import { CommonModule } from '@angular/common';
import { AuthService } from '../../dependencies/services/auth.service';

@Component({
  selector: 'app-container',
  imports: [CommonModule, RouterModule],
  templateUrl: './container.html',
  styleUrl: './container.scss',
})
export class Container {
  menus: IMENU[] = [];
  isSideBarOpen: boolean = true;
  isMobileSidebarOpen: boolean = false;
  isMobileOverlayActive: boolean = false;
  isUserDropdownOpen: boolean = false;
  sidebarWidth: number = 220;
  sidebarClosedWidth: number = 55;
  isMobileView: boolean = false;
  @ViewChild('logoutModal') logoutModal: any;
  showLogoutModal = false;
  userData: any = { name: '', email: '' };

  // Track open states for menus and submenus
  openMenus: { [key: string]: boolean } = {};
  openSubMenus: { [key: string]: boolean } = {};

  readonly authService = inject(AuthService)

  constructor() { }

  ngOnInit(): void {
    this.checkScreenSize();
    this.menus = MENU;
    this.getUserData();
  }

  getUserData(): void {
    try {
      const userDataString = sessionStorage.getItem('userDetails');
      if (userDataString) {
        this.userData = JSON.parse(userDataString);
      } else {
        this.userData = { name: 'Guest', email: 'No email' };
      }
    } catch (error) {
      this.userData = { name: 'Error loading data', email: 'Error' };
    }
  }

  getUserInitials(): string {
    if (this.userData.name) {
      return this.userData.name.charAt(0).toUpperCase();
    }
    return 'U';
  }

  toggleUserDropdown(): void {
    this.isUserDropdownOpen = !this.isUserDropdownOpen;
  }

  closeUserDropdown(): void {
    this.isUserDropdownOpen = false;
  }

  openLogoutConfirmation(): void {
    this.closeUserDropdown();
    this.showLogoutModal = true;
  }

  closeLogoutModal(): void {
    this.showLogoutModal = false;
  }

  confirmLogout(): void {
    this.showLogoutModal = false;
    this.authService.logout();
  }

  @HostListener('window:resize', ['$event'])
  onResize(event: any) {
    this.checkScreenSize();
  }

  @HostListener('document:click', ['$event'])
  onDocumentClick(event: MouseEvent): void {
    const target = event.target as HTMLElement;
    if (!target.closest('.user-profile-dropdown')) {
      this.closeUserDropdown();
    }
  }

  checkScreenSize() {
    this.isMobileView = window.innerWidth <= 992;
    if (this.isMobileView) {
      this.isSideBarOpen = false;
      this.isMobileSidebarOpen = false;
      this.isMobileOverlayActive = false;
    } else {
      this.isSideBarOpen = true;
      this.isMobileSidebarOpen = false;
      this.isMobileOverlayActive = false;
    }
  }

  toggleSidebar() {
    if (this.isMobileView) {
      this.isMobileSidebarOpen = !this.isMobileSidebarOpen;
      this.isMobileOverlayActive = this.isMobileSidebarOpen;
    } else {
      this.isSideBarOpen = !this.isSideBarOpen;
    }
  }

  closeMobileSidebar() {
    if (this.isMobileView) {
      this.isMobileSidebarOpen = false;
      this.isMobileOverlayActive = false;
    }
  }

  onMobileLinkClick() {
    if (this.isMobileView) {
      this.closeMobileSidebar();
    }
  }

  // New methods for menu toggling
  toggleMenu(menuIndex: number): void {
    const key = `menu_${menuIndex}`;
    this.openMenus[key] = !this.openMenus[key];

    // Close this menu's submenus when closing the menu
    if (!this.openMenus[key]) {
      this.closeSubMenusForMenu(menuIndex);
    }
  }

  toggleSubMenu(menuIndex: number, subIndex: number): void {
    const key = `submenu_${menuIndex}_${subIndex}`;
    this.openSubMenus[key] = !this.openSubMenus[key];
  }

  isMenuOpen(menuIndex: number): boolean {
    return !!this.openMenus[`menu_${menuIndex}`];
  }

  isSubMenuOpen(menuIndex: number, subIndex: number): boolean {
    return !!this.openSubMenus[`submenu_${menuIndex}_${subIndex}`];
  }

  closeSubMenusForMenu(menuIndex: number): void {
    // Close all submenus for this menu
    Object.keys(this.openSubMenus).forEach(key => {
      if (key.startsWith(`submenu_${menuIndex}_`)) {
        this.openSubMenus[key] = false;
      }
    });
  }
}
