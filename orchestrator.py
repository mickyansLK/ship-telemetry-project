#!/usr/bin/env python3
"""
Ship Telemetry Pipeline Orchestrator
Coordinates the execution of ship simulation and stream processing components.
"""

import subprocess
import time
import signal
import sys
import os
import logging
from datetime import datetime
from pathlib import Path
import threading
import queue
import psutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('orchestrator.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ProcessManager:
    """Manages subprocess execution with monitoring and cleanup"""
    
    def __init__(self):
        self.processes = {}
        self.running = True
        self.output_queues = {}
        
    def start_process(self, name, command, cwd=None, env=None):
        """Start a subprocess and monitor its output"""
        try:
            logger.info(f"🚀 Starting {name}...")
            
            # Create output queue for this process
            self.output_queues[name] = queue.Queue()
            
            # Start the process
            process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                cwd=cwd,
                env=env,
                preexec_fn=os.setsid  # Create new process group
            )
            
            self.processes[name] = process
            
            # Start output monitoring thread
            output_thread = threading.Thread(
                target=self._monitor_output,
                args=(name, process),
                daemon=True
            )
            output_thread.start()
            
            logger.info(f"✅ {name} started with PID {process.pid}")
            return process
            
        except Exception as e:
            logger.error(f"❌ Failed to start {name}: {str(e)}")
            return None
    
    def _monitor_output(self, name, process):
        """Monitor process output and log it"""
        try:
            for line in iter(process.stdout.readline, ''):
                if line.strip():
                    logger.info(f"[{name}] {line.strip()}")
                    self.output_queues[name].put(line.strip())
        except Exception as e:
            logger.error(f"Error monitoring {name} output: {str(e)}")
    
    def is_process_running(self, name):
        """Check if a process is still running"""
        if name not in self.processes:
            return False
        
        process = self.processes[name]
        return process.poll() is None
    
    def wait_for_process_ready(self, name, ready_indicator, timeout=60):
        """Wait for a process to be ready based on output indicator"""
        logger.info(f"⏳ Waiting for {name} to be ready...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if not self.is_process_running(name):
                logger.error(f"❌ {name} stopped unexpectedly")
                return False
            
            try:
                # Check recent output for ready indicator
                while not self.output_queues[name].empty():
                    line = self.output_queues[name].get_nowait()
                    if ready_indicator in line:
                        logger.info(f"✅ {name} is ready!")
                        return True
            except queue.Empty:
                pass
            
            time.sleep(1)
        
        logger.warning(f"⚠️ {name} ready timeout after {timeout}s")
        return False
    
    def stop_process(self, name):
        """Stop a specific process gracefully"""
        if name not in self.processes:
            return
        
        process = self.processes[name]
        if process.poll() is None:  # Process is still running
            logger.info(f"🛑 Stopping {name}...")
            try:
                # Send SIGTERM to process group
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                
                # Wait for graceful shutdown
                try:
                    process.wait(timeout=10)
                    logger.info(f"✅ {name} stopped gracefully")
                except subprocess.TimeoutExpired:
                    # Force kill if needed
                    logger.warning(f"⚠️ Force killing {name}")
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                    process.wait()
                    
            except Exception as e:
                logger.error(f"❌ Error stopping {name}: {str(e)}")
        
        del self.processes[name]
    
    def stop_all(self):
        """Stop all managed processes"""
        logger.info("🛑 Stopping all processes...")
        self.running = False
        
        for name in list(self.processes.keys()):
            self.stop_process(name)
        
        logger.info("✅ All processes stopped")

class ShipTelemetryOrchestrator:
    """Main orchestrator for the ship telemetry pipeline"""
    
    def __init__(self):
        self.process_manager = ProcessManager()
        self.project_root = Path(__file__).parent
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"📡 Received signal {signum}, shutting down...")
        self.shutdown()
        sys.exit(0)
    
    def check_prerequisites(self):
        """Check if all required components are available"""
        logger.info("🔍 Checking prerequisites...")
        
        required_files = [
            'simulate_sensors.py',
            'stream_processor.py',
            'docker-compose.yml'
        ]
        
        missing_files = []
        for file in required_files:
            if not (self.project_root / file).exists():
                missing_files.append(file)
        
        if missing_files:
            logger.error(f"❌ Missing required files: {missing_files}")
            return False
        
        # Check if Docker is running
        try:
            result = subprocess.run(['docker', 'info'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                logger.error("❌ Docker is not running")
                return False
        except Exception as e:
            logger.error(f"❌ Docker check failed: {str(e)}")
            return False
        
        logger.info("✅ All prerequisites met")
        return True
    
    def start_kafka_services(self):
        """Start Kafka and Zookeeper services"""
        logger.info("🐳 Starting Kafka services...")
        
        # Stop any existing containers
        subprocess.run(['docker-compose', 'down'], 
                      cwd=self.project_root, capture_output=True)
        
        # Start services
        process = self.process_manager.start_process(
            "kafka-services",
            "docker-compose up",
            cwd=self.project_root
        )
        
        if not process:
            return False
        
        # Wait for Kafka to be ready
        return self.process_manager.wait_for_process_ready(
            "kafka-services", 
            "started (kafka.server.KafkaServer)",
            timeout=120
        )
    
    def start_ship_simulation(self):
        """Start the ship sensor simulation"""
        logger.info("🚢 Starting ship simulation...")
        
        # Create virtual environment path
        venv_python = self.project_root / "venv" / "bin" / "python"
        if not venv_python.exists():
            venv_python = "python"  # Fallback to system python
        
        process = self.process_manager.start_process(
            "ship-simulation",
            f"{venv_python} simulate_sensors.py",
            cwd=self.project_root
        )
        
        if not process:
            return False
        
        # Wait for simulation to start producing data
        return self.process_manager.wait_for_process_ready(
            "ship-simulation",
            "Sent telemetry data",
            timeout=30
        )
    
    def start_stream_processor(self):
        """Start the stream processor"""
        logger.info("⚡ Starting stream processor...")
        
        # Create virtual environment path
        venv_python = self.project_root / "venv" / "bin" / "python"
        if not venv_python.exists():
            venv_python = "python"  # Fallback to system python
        
        process = self.process_manager.start_process(
            "stream-processor",
            f"{venv_python} stream_processor.py",
            cwd=self.project_root
        )
        
        if not process:
            return False
        
        # Wait for stream processor to start
        return self.process_manager.wait_for_process_ready(
            "stream-processor",
            "Stream processing started successfully",
            timeout=60
        )
    
    def monitor_pipeline(self):
        """Monitor the running pipeline"""
        logger.info("👀 Monitoring pipeline...")
        
        while self.process_manager.running:
            try:
                # Check if all processes are still running
                processes_status = {}
                for name in ["kafka-services", "ship-simulation", "stream-processor"]:
                    processes_status[name] = self.process_manager.is_process_running(name)
                
                # Log status every 30 seconds
                running_count = sum(processes_status.values())
                logger.info(f"📊 Pipeline status: {running_count}/3 processes running")
                
                # If any critical process died, restart it
                if not processes_status.get("ship-simulation", False):
                    logger.warning("⚠️ Ship simulation stopped, restarting...")
                    self.start_ship_simulation()
                
                if not processes_status.get("stream-processor", False):
                    logger.warning("⚠️ Stream processor stopped, restarting...")
                    self.start_stream_processor()
                
                time.sleep(30)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"❌ Error in monitoring: {str(e)}")
                time.sleep(5)
    
    def run_pipeline(self):
        """Run the complete telemetry pipeline"""
        logger.info("🎬 Starting Ship Telemetry Pipeline Orchestrator")
        
        try:
            # Check prerequisites
            if not self.check_prerequisites():
                logger.error("❌ Prerequisites not met, exiting")
                return False
            
            # Start services in order
            logger.info("📋 Starting pipeline components...")
            
            # 1. Start Kafka services
            if not self.start_kafka_services():
                logger.error("❌ Failed to start Kafka services")
                return False
            
            # Wait a bit for Kafka to fully initialize
            time.sleep(10)
            
            # 2. Start stream processor (consumer should be ready before producer)
            if not self.start_stream_processor():
                logger.error("❌ Failed to start stream processor")
                return False
            
            # Wait a bit for stream processor to initialize
            time.sleep(5)
            
            # 3. Start ship simulation (producer)
            if not self.start_ship_simulation():
                logger.error("❌ Failed to start ship simulation")
                return False
            
            logger.info("🎉 Pipeline started successfully!")
            logger.info("📊 You can now run the dashboard: streamlit run monitoring_dashboard.py")
            
            # Monitor the pipeline
            self.monitor_pipeline()
            
        except Exception as e:
            logger.error(f"❌ Pipeline error: {str(e)}")
            return False
        finally:
            self.shutdown()
        
        return True
    
    def shutdown(self):
        """Gracefully shutdown the pipeline"""
        logger.info("🔄 Shutting down pipeline...")
        self.process_manager.stop_all()
        
        # Stop Docker services
        try:
            subprocess.run(['docker-compose', 'down'], 
                          cwd=self.project_root, capture_output=True, timeout=30)
            logger.info("✅ Docker services stopped")
        except Exception as e:
            logger.error(f"❌ Error stopping Docker services: {str(e)}")

def main():
    """Main entry point"""
    orchestrator = ShipTelemetryOrchestrator()
    
    try:
        success = orchestrator.run_pipeline()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("👋 Orchestrator interrupted by user")
        orchestrator.shutdown()
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Orchestrator failed: {str(e)}")
        orchestrator.shutdown()
        sys.exit(1)

if __name__ == "__main__":
    main() 