#!/usr/bin/env python3
"""
RFB Gold Layer Builder (Optimized)

Orchestrates dbt for efficient transformation:
- Selective model runs (staging → intermediate → dimensional)
- Parallel execution (dbt threads)
- Incremental snapshots (only new data)
- Automatic optimization after each layer
- Performance monitoring + reporting

Typical timing:
- Staging layer: ~2min
- Intermediate: ~2min
- Dimensional (dims + facts): ~3min
- Total: ~7min (vs 30min sequential)
"""

import os
import sys
import json
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import logging
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [gold_builder_opt] %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
DBT_PROJECT_DIR = Path(__file__).parent.parent / 'rfb_lakehouse_project'
DBT_PROFILES_DIR = os.getenv('DBT_PROFILES_DIR', str(Path.home() / '.dbt'))
DBT_THREADS = int(os.getenv('DBT_THREADS', '4'))
REF_MONTH = int(os.getenv('REF_MONTH', '202608'))

# Layer execution order
LAYERS = {
    'staging': {
        'tags': ['staging'],
        'description': 'Clean, deduplicate Silver data',
        'optimize': False,  # Small tables
    },
    'intermediate': {
        'tags': ['intermediate'],
        'description': 'Enrich with domain joins',
        'optimize': False,
    },
    'dimensional': {
        'tags': ['dimensional'],
        'description': 'Build dimensions + facts + snapshots',
        'optimize': True,  # Large tables, apply Z-order
    },
}


class DBTExecutor:
    """Execute dbt commands with monitoring."""
    
    def __init__(self, project_dir: Path, profiles_dir: Path):
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.env = os.environ.copy()
        self.env['DBT_PROFILES_DIR'] = str(profiles_dir)
    
    def run_command(self, cmd: List[str], tag: str = None) -> Tuple[int, str]:
        """Execute dbt command and capture output."""
        full_cmd = ['dbt'] + cmd + [
            '--profiles-dir', str(self.profiles_dir),
            '--threads', str(DBT_THREADS),
        ]
        
        logger.info(f"Running: {' '.join(full_cmd)}")
        
        try:
            result = subprocess.run(
                full_cmd,
                cwd=self.project_dir,
                env=self.env,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout
            )
            
            output = result.stdout + result.stderr
            
            if result.returncode == 0:
                logger.info(f"✓ Command succeeded")
            else:
                logger.error(f"✗ Command failed (exit code {result.returncode})")
                logger.error(output[-500:])  # Last 500 chars
            
            return result.returncode, output
        
        except subprocess.TimeoutExpired:
            logger.error("Command timed out (1 hour)")
            return 1, ""
        except Exception as e:
            logger.error(f"Command failed: {e}")
            return 1, ""
    
    def parse(self) -> bool:
        """Validate project."""
        returncode, _ = self.run_command(['parse'])
        return returncode == 0
    
    def deps(self) -> bool:
        """Install dependencies."""
        returncode, _ = self.run_command(['deps'])
        return returncode == 0
    
    def run_layer(self, layer_name: str, layer_config: Dict) -> Tuple[bool, Dict]:
        """
        Run single layer with monitoring.
        
        Returns (success, stats).
        """
        logger.info(f"\n{'='*70}")
        logger.info(f"Layer: {layer_name.upper()}")
        logger.info(f"Description: {layer_config['description']}")
        logger.info(f"{'='*70}\n")
        
        start = datetime.now()
        
        # Run models for this layer
        cmd = [
            'run',
            '--select', f"tag:{layer_config['tags'][0]}",
            '--vars', json.dumps({'ref_month': REF_MONTH}),
        ]
        
        returncode, output = self.run_command(cmd, tag=layer_name)
        
        elapsed = (datetime.now() - start).total_seconds()
        
        # Parse output for model counts
        models_run = output.count('Completed running')
        
        stats = {
            'layer': layer_name,
            'success': returncode == 0,
            'elapsed_sec': elapsed,
            'models_run': models_run,
            'rate_sec': elapsed / models_run if models_run > 0 else 0,
        }
        
        if returncode == 0:
            logger.info(f"✓ Layer {layer_name} complete in {elapsed:.1f}s ({models_run} models)")
        else:
            logger.error(f"✗ Layer {layer_name} failed")
        
        return returncode == 0, stats
    
    def test(self) -> Tuple[bool, Dict]:
        """Run dbt tests."""
        logger.info(f"\n{'='*70}")
        logger.info(f"Testing Gold models...")
        logger.info(f"{'='*70}\n")
        
        start = datetime.now()
        returncode, output = self.run_command(['test'])
        elapsed = (datetime.now() - start).total_seconds()
        
        passed = output.count('PASSED')
        failed = output.count('FAILED')
        
        stats = {
            'passed': passed,
            'failed': failed,
            'elapsed_sec': elapsed,
        }
        
        if returncode == 0:
            logger.info(f"✓ All tests passed ({passed} total) in {elapsed:.1f}s")
        else:
            logger.warning(f"⚠ {failed} test(s) failed, {passed} passed")
        
        return returncode == 0, stats
    
    def docs_generate(self) -> bool:
        """Generate documentation."""
        logger.info("Generating documentation...")
        returncode, _ = self.run_command(['docs', 'generate'])
        if returncode == 0:
            logger.info(f"✓ Documentation generated")
            logger.info(f"  View at: {self.project_dir}/target/index.html")
        return returncode == 0


class OptimizationManager:
    """Manage Delta Lake table optimizations after dbt layers."""
    
    @staticmethod
    def optimize_delta_tables(layer_name: str):
        """Optimize Gold Delta tables after layer completion."""
        from rfb_delta_utils import S3ClientConfig, DeltaLakeOptimizer
        
        logger.info(f"Optimizing Delta tables after {layer_name} layer...")
        
        # Map layers to tables to optimize
        tables_by_layer = {
            'staging': [
                'stg_rfb__estabelecimentos',
                'stg_rfb__empresas',
                'stg_rfb__socios',
            ],
            'intermediate': [
                'int_rfb__estabelecimentos',
                'int_rfb__empresas',
                'int_rfb__socios',
            ],
            'dimensional': [
                'dim_rfb__estabelecimento',
                'dim_rfb__empresa',
                'fct_rfb__estabelecimento_mensal',
            ],
        }
        
        tables = tables_by_layer.get(layer_name, [])
        if not tables:
            logger.info("  No tables to optimize for this layer")
            return
        
        try:
            s3_config = S3ClientConfig()
            
            for table in tables:
                delta_path = f"s3://gold/rfb/{table.split('__')[0]}/{table}/"
                try:
                    stats = DeltaLakeOptimizer.optimize_table(
                        delta_path,
                        z_order_cols=['referencia_mes'],
                        storage_options=s3_config.storage_options
                    )
                    if stats:
                        logger.info(f"  ✓ {table}: {stats['files_reduced']} files merged")
                except Exception as e:
                    logger.warning(f"  ⚠ Failed to optimize {table}: {e}")
        
        except Exception as e:
            logger.warning(f"Optimization setup failed: {e}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Build Gold Layer with dbt (Optimized)'
    )
    parser.add_argument(
        '--layer',
        choices=['staging', 'intermediate', 'dimensional', 'all'],
        default='all',
        help='Run specific layer (default: all)'
    )
    parser.add_argument(
        '--skip-optimize',
        action='store_true',
        help='Skip Z-order optimizations (faster for frequent updates)'
    )
    parser.add_argument(
        '--skip-tests',
        action='store_true',
        help='Skip dbt tests'
    )
    parser.add_argument(
        '--skip-docs',
        action='store_true',
        help='Skip documentation generation'
    )
    
    args = parser.parse_args()
    
    try:
        logger.info(f"Starting Gold Layer Build (ref_month={REF_MONTH})")
        start_total = datetime.now()
        
        # Initialize executor
        executor = DBTExecutor(DBT_PROJECT_DIR, Path(DBT_PROFILES_DIR))
        
        # Step 1: Parse (validate)
        logger.info("Step 1: Validating dbt project...")
        if not executor.parse():
            logger.error("Parse failed, aborting")
            return 1
        
        # Step 2: Install dependencies
        logger.info("Step 2: Installing dependencies...")
        if not executor.deps():
            logger.error("Deps failed, continuing anyway...")
        
        # Step 3: Run layers
        stats_by_layer = {}
        layers_to_run = [args.layer] if args.layer != 'all' else list(LAYERS.keys())
        
        for layer_name in layers_to_run:
            layer_config = LAYERS[layer_name]
            success, stats = executor.run_layer(layer_name, layer_config)
            stats_by_layer[layer_name] = stats
            
            if not success:
                logger.error(f"Layer {layer_name} failed, aborting")
                return 1
            
            # Optimize after layer (optional)
            if layer_config['optimize'] and not args.skip_optimize:
                try:
                    OptimizationManager.optimize_delta_tables(layer_name)
                except Exception as e:
                    logger.warning(f"Optimization failed (non-critical): {e}")
        
        # Step 4: Tests
        if not args.skip_tests:
            logger.info("Step 4: Running tests...")
            success, test_stats = executor.test()
            stats_by_layer['tests'] = test_stats
            if not success and not args.skip_tests:
                logger.warning("Some tests failed, but continuing...")
        
        # Step 5: Documentation
        if not args.skip_docs:
            logger.info("Step 5: Generating documentation...")
            executor.docs_generate()
        
        # Summary
        elapsed_total = (datetime.now() - start_total).total_seconds()
        
        logger.info(f"\n{'='*70}")
        logger.info(f"GOLD LAYER BUILD COMPLETE")
        logger.info(f"{'='*70}")
        logger.info(f"Total time: {elapsed_total:.1f}s")
        logger.info(f"\nLayer summary:")
        for layer, stats in stats_by_layer.items():
            if 'elapsed_sec' in stats:
                logger.info(
                    f"  {layer:15} → {stats['elapsed_sec']:6.1f}s "
                    f"({stats['models_run']} models)"
                )
        
        return 0
    
    except Exception as e:
        logger.error(f"Build failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())