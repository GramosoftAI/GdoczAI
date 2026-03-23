#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Service Startup Script for Document Processing Pipeline

OPTIMIZED VERSION - Fast Logging:
- Direct stdout/stderr (no buffering delays)
- Async log streaming with queue
- Separate log files per service
- Color-coded console output
- Real-time performance monitoring
"""

import argparse
import subprocess
import sys
import time
import signal
import os
import threading
import queue
from pathlib import Path
from typing import List, Optional
from datetime import datetime


# ANSI Color codes for better readability
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class LogQueue:
    """Thread-safe queue for fast log processing"""
    
    def __init__(self, max_size: int = 10000):
        self.queue = queue.Queue(maxsize=max_size)
        self.running = True
    
    def put(self, message: str):
        """Add message to queue (non-blocking)"""
        try:
            self.queue.put_nowait(message)
        except queue.Full:
            # Drop oldest message if queue is full
            try:
                self.queue.get_nowait()
                self.queue.put_nowait(message)
            except:
                pass
    
    def get(self, timeout: float = 0.1) -> Optional[str]:
        """Get message from queue"""
        try:
            return self.queue.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def stop(self):
        """Stop queue processing"""
        self.running = False


class ServiceManager:
    """Manages pipeline services with optimized real-time logging"""
    
    def __init__(self, show_logs: bool = True, colored: bool = True):
        self.processes: List[subprocess.Popen] = []
        self.running = True
        self.show_logs = show_logs
        self.colored = colored
        self.log_queues = {}
        self.log_threads = []
        
        # Create logs directory
        Path("logs").mkdir(exist_ok=True)
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"\n{Colors.WARNING}Shutting down services (signal {signum})...{Colors.ENDC}")
        self.running = False
        self.stop_all_services()
        sys.exit(0)
    
    def colorize(self, text: str, color: str) -> str:
        """Add color to text if enabled"""
        if self.colored and sys.stdout.isatty():
            return f"{color}{text}{Colors.ENDC}"
        return text
    
    def get_service_color(self, name: str) -> str:
        """Get color for service name"""
        colors = {
            "Pipeline": Colors.OKGREEN,
            "API": Colors.OKBLUE,
            "Dashboard": Colors.OKCYAN
        }
        for key, color in colors.items():
            if key in name:
                return color
        return Colors.ENDC
    
    def stream_output_fast(self, process: subprocess.Popen, name: str, log_file: Path):
        """
        OPTIMIZED: Stream output with minimal buffering
        
        Key optimizations:
        1. No line-by-line processing
        2. Batch writes to file
        3. Async queue for console output
        4. Direct stderr to stdout
        """
        log_queue = LogQueue()
        self.log_queues[name] = log_queue
        
        # Open log file once
        with open(log_file, 'a', buffering=8192) as f:  # 8KB buffer
            try:
                while self.running and process.poll() is None:
                    # Read in chunks (faster than readline)
                    line = process.stdout.readline()
                    
                    if line:
                        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        log_line = f"[{timestamp}] {line.rstrip()}\n"
                        
                        # Write to file (buffered)
                        f.write(log_line)
                        
                        # Queue for console (non-blocking)
                        if self.show_logs:
                            log_queue.put((name, line.rstrip()))
            
            except Exception as e:
                print(f"Error streaming {name}: {e}")
            
            finally:
                log_queue.stop()
    
    def console_writer_thread(self):
        """
        OPTIMIZED: Single thread writes all logs to console
        
        Benefits:
        - Reduces context switching
        - Better console performance
        - Batch processing
        """
        while self.running or any(q.running for q in self.log_queues.values()):
            messages_written = 0
            
            for name, log_queue in self.log_queues.items():
                # Process multiple messages per iteration
                for _ in range(10):  # Batch up to 10 messages
                    message = log_queue.get(timeout=0.01)
                    if message:
                        service_name, log_text = message
                        color = self.get_service_color(service_name)
                        prefix = self.colorize(f"[{service_name}]", color)
                        print(f"{prefix} {log_text}", flush=True)
                        messages_written += 1
                    else:
                        break
            
            # Small sleep if no messages
            if messages_written == 0:
                time.sleep(0.05)
    
    def start_service(
        self, 
        command: List[str], 
        name: str,
        log_file: Optional[str] = None,
        wait_time: int = 3
    ) -> Optional[subprocess.Popen]:
        """
        Start a service with optimized logging
        
        OPTIMIZATIONS:
        - unbuffered output (-u flag for Python)
        - Direct stdout/stderr merge
        - Separate log file per service
        """
        try:
            print(f"\n{self.colorize('=' * 80, Colors.HEADER)}")
            print(f"{self.colorize(f'Starting {name}...', Colors.BOLD)}")
            print(f"{self.colorize('=' * 80, Colors.HEADER)}")
            
            # Determine log file
            if log_file is None:
                log_file = f"logs/{name.lower().replace(' ', '_')}.log"
            
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Add Python unbuffered flag if command is Python
            if command[0] == sys.executable:
                command.insert(1, '-u')  # Unbuffered output
            
            # Set environment for unbuffered output
            env = os.environ.copy()
            env['PYTHONUNBUFFERED'] = '1'
            
            # Start process with optimized settings
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # Merge stderr to stdout
                text=True,
                bufsize=1,  # Line buffered
                universal_newlines=True,
                env=env
            )
            
            self.processes.append(process)
            
            # Start output streaming thread
            stream_thread = threading.Thread(
                target=self.stream_output_fast,
                args=(process, name, log_path),
                daemon=True,
                name=f"Stream-{name}"
            )
            stream_thread.start()
            self.log_threads.append(stream_thread)
            
            time.sleep(wait_time)
            
            # Check if process is still running
            if process.poll() is None:
                success_msg = f"SUCCESS: {name} started (PID: {process.pid})"
                print(f"\n{self.colorize(success_msg, Colors.OKGREEN)}")
                print(f"   Log file: {log_path}")
                return process
            else:
                fail_msg = f"FAILED: {name} failed to start"
                print(f"\n{self.colorize(fail_msg, Colors.FAIL)}")
                return None
                
        except Exception as e:
            print(f"{self.colorize(f'ERROR: Failed to start {name}: {e}', Colors.FAIL)}")
            return None
    
    def stop_all_services(self):
        """Stop all running services"""
        print(f"\n{self.colorize('Stopping all services...', Colors.WARNING)}")
        
        for process in self.processes:
            if process.poll() is None:
                try:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                        print(f"Service (PID: {process.pid}) stopped gracefully")
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                        print(f"Service (PID: {process.pid}) force killed")
                except Exception as e:
                    print(f"Error stopping service (PID: {process.pid}): {e}")
        
        # Stop log queues
        for log_queue in self.log_queues.values():
            log_queue.stop()
        
        self.processes.clear()
    
    def wait_for_services(self):
        """Wait for all services and monitor them"""
        print(f"\n{self.colorize('=' * 80, Colors.HEADER)}")
        print(f"{self.colorize('Monitoring services... (Press Ctrl+C to stop)', Colors.BOLD)}")
        print(f"{self.colorize('=' * 80, Colors.HEADER)}")
        
        # Start console writer thread
        console_thread = threading.Thread(
            target=self.console_writer_thread,
            daemon=True,
            name="ConsoleWriter"
        )
        console_thread.start()
        
        print("\nLog files:")
        for name, _ in self.log_queues.items():
            log_file = f"logs/{name.lower().replace(' ', '_')}.log"
            print(f"   * {name}: {log_file}")
        
        print(f"\n{self.colorize('Tip:', Colors.OKCYAN)} Run 'tail -f logs/*.log' to see all logs")
        print(f"{self.colorize('=' * 80, Colors.HEADER)}")
        
        try:
            last_check = time.time()
            
            while self.running and self.processes:
                # Check if any process has died
                for i, process in enumerate(self.processes[:]):
                    if process.poll() is not None:
                        warning = f"WARNING: Service (PID: {process.pid}) has stopped"
                        print(f"\n{self.colorize(warning, Colors.WARNING)}")
                        self.processes.remove(process)
                
                if not self.processes:
                    print("\nAll services have stopped")
                    break
                
                # Performance monitoring every 30 seconds
                if time.time() - last_check > 30:
                    self.show_performance_stats()
                    last_check = time.time()
                
                time.sleep(5)
                
        except KeyboardInterrupt:
            print(f"\n{self.colorize('Keyboard interrupt received', Colors.WARNING)}")
        finally:
            self.stop_all_services()
    
    def show_performance_stats(self):
        """Show performance statistics"""
        stats = []
        for name, log_queue in self.log_queues.items():
            queue_size = log_queue.queue.qsize()
            stats.append(f"{name}: {queue_size} queued")
        
        if stats:
            print(f"\n{self.colorize('?? Performance:', Colors.OKCYAN)} {', '.join(stats)}")
    
    def start_pipeline_monitor(self, config_file: str = "config/config.yaml"):
        """Start the main pipeline monitoring"""
        command = [sys.executable, "-m", "src.services.pipeline.document_pipeline_scheduler"]
        return self.start_service(
            command, 
            "Pipeline Monitor",
            log_file="logs/pipeline.log"
        )
    
    def start_api_server(
        self, 
        host: str = "0.0.0.0", 
        port: int = 4535, 
        config_file: str = "config/config.yaml"
    ):
        """Start the FastAPI server"""
        command = [
            sys.executable, "-m", "src.api.api_server", 
            "--host", host,
            "--port", str(port),
            "--config", config_file
        ]

        return self.start_service(
            command, 
            f"API Server",
            log_file="logs/api_server.log"
        )
    
    def start_dashboard(
        self, 
        host: str = "127.0.0.1", 
        port: int = 5000, 
        config_file: str = "config/config.yaml"
    ):
        """Start the monitoring dashboard"""
        command = [
            sys.executable, "-m", "src.monitoring.monitor_dashboard",
            "--mode", "web",
            "--host", host,
            "--port", str(port),
            "--config", config_file
        ]
        return self.start_service(
            command, 
            f"Dashboard",
            log_file="logs/dashboard.log"
        )
    
    def check_dependencies(self):
        """Check if required files exist"""
        required_files = {
            "src/services/pipeline/document_pipeline_scheduler.py": "Pipeline Monitor",
            "src/api/api_server.py": "API Server",
            "config/config.yaml": "Configuration"
        }
        
        missing_files = []
        for file, desc in required_files.items():
            if not Path(file).exists():
                missing_files.append(f"{file} ({desc})")
        
        if missing_files:
            print(f"{self.colorize('ERROR: Missing required files:', Colors.FAIL)}")
            for file in missing_files:
                print(f"   - {file}")
            return False
        
        return True
    
    def show_service_info(self):
        """Show information about available services"""
        print(f"{self.colorize('Available Services:', Colors.BOLD)}")
        print("   * Pipeline Monitor: Monitors SFTP folder and processes files")
        print("   * API Server: REST API for file processing and management")
        print("   * Dashboard: Web-based monitoring dashboard (optional)")
        print()
        print(f"{self.colorize('Default URLs:', Colors.BOLD)}")
        print("   * API Server: http://localhost:4535")
        print("   * API Docs: http://localhost:4535/docs")
        print("   * Dashboard: http://localhost:5000")
        print()
        print(f"{self.colorize('Log Files:', Colors.BOLD)}")
        print("   * logs/pipeline_monitor.log - Pipeline operations")
        print("   * logs/api_server.log - API requests")
        print("   * logs/dashboard.log - Dashboard activity")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Start Document Processing Pipeline Services (OPTIMIZED)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --all                    # Start all services
  %(prog)s --pipeline               # Start only pipeline monitoring
  %(prog)s --api                    # Start only API server
  %(prog)s --dashboard              # Start only dashboard
  %(prog)s --api --dashboard        # Start API and dashboard
  %(prog)s --api --port 8080        # Start API on custom port
  %(prog)s --no-logs                # Start without showing logs in console
  %(prog)s --no-color               # Disable colored output
        """
    )
    
    # Service selection
    parser.add_argument('--all', action='store_true', help='Start all services')
    parser.add_argument('--pipeline', action='store_true', help='Start pipeline monitoring')
    parser.add_argument('--api', action='store_true', help='Start API server')
    parser.add_argument('--dashboard', action='store_true', help='Start monitoring dashboard')
    
    # Configuration
    parser.add_argument('--config', default='config/config.yaml', help='Configuration file path')
    parser.add_argument('--api-host', default='0.0.0.0', help='API server host')
    parser.add_argument('--api-port', type=int, default=4535, help='API server port')
    parser.add_argument('--dashboard-host', default='127.0.0.1', help='Dashboard host')
    parser.add_argument('--dashboard-port', type=int, default=5000, help='Dashboard port')
    
    # Options
    parser.add_argument('--info', action='store_true', help='Show service information and exit')
    parser.add_argument('--check', action='store_true', help='Check dependencies and exit')
    parser.add_argument('--no-logs', action='store_true', help='Do not show logs in console')
    parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    
    args = parser.parse_args()
    
    manager = ServiceManager(
        show_logs=not args.no_logs,
        colored=not args.no_color
    )
    
    # Show info and exit
    if args.info:
        manager.show_service_info()
        return
    
    # Check dependencies
    if not manager.check_dependencies():
        print("\nEnsure all required files are present")
        return
    
    if args.check:
        print(f"{manager.colorize('SUCCESS: All dependencies are available', Colors.OKGREEN)}")
        return
    
    # Determine which services to start
    start_pipeline = args.all or args.pipeline
    start_api = args.all or args.api
    start_dashboard = args.all or args.dashboard
    
    # If no specific service is requested, show help
    if not (start_pipeline or start_api or start_dashboard):
        parser.print_help()
        print(f"\n{manager.colorize('Tip:', Colors.OKCYAN)} Use --all to start all services")
        return
    
    print(f"{manager.colorize('=' * 80, Colors.HEADER)}")
    print(f"{manager.colorize('Document Processing Pipeline - Service Manager (OPTIMIZED)', Colors.BOLD)}")
    print(f"{manager.colorize('=' * 80, Colors.HEADER)}")
    print(f"Configuration: {args.config}")
    print(f"Console logs: {manager.colorize('ENABLED', Colors.OKGREEN) if not args.no_logs else manager.colorize('DISABLED', Colors.WARNING)}")
    print(f"File logs: {manager.colorize('ENABLED', Colors.OKGREEN)} in logs/ directory")
    print(f"Colored output: {manager.colorize('ENABLED', Colors.OKGREEN) if not args.no_color else manager.colorize('DISABLED', Colors.WARNING)}")
    print(f"{manager.colorize('=' * 80, Colors.HEADER)}")
    
    services_started = 0
    
    # Start API server first
    if start_api:
        if manager.start_api_server(args.api_host, args.api_port, args.config):
            services_started += 1
            print("\nWaiting 5 seconds for API server to be ready...")
            time.sleep(5)
    
    # Then start pipeline
    if start_pipeline:
        if manager.start_pipeline_monitor(args.config):
            services_started += 1
    
    # Finally start dashboard
    if start_dashboard:
        if manager.start_dashboard(args.dashboard_host, args.dashboard_port, args.config):
            services_started += 1
    
    if services_started == 0:
        print(f"\n{manager.colorize('ERROR: No services started successfully', Colors.FAIL)}")
        return
    
    print(f"\n{manager.colorize('=' * 80, Colors.HEADER)}")
    success_msg = f"SUCCESS: {services_started} service(s) started successfully"
    print(f"{manager.colorize(success_msg, Colors.OKGREEN)}")
    print(f"{manager.colorize('=' * 80, Colors.HEADER)}")
    print("\nService URLs:")
    
    if start_api:
        print(f"   * API Server: http://{args.api_host}:{args.api_port}")
        print(f"   * API Docs: http://{args.api_host}:{args.api_port}/docs")
    
    if start_dashboard:
        print(f"   * Dashboard: http://{args.dashboard_host}:{args.dashboard_port}")
    
    # Monitor services
    manager.wait_for_services()


if __name__ == "__main__":
    main()