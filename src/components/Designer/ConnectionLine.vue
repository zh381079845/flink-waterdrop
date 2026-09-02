<template>
  <svg class="connection-line" :style="lineStyle">
    <line 
      :x1="source.x" 
      :y1="source.y" 
      :x2="target.x" 
      :y2="target.y"
      class="connection-path"
    />
    <circle 
      :cx="midPoint.x" 
      :cy="midPoint.y" 
      r="5" 
      class="connection-handle"
      @mousedown="onHandleMouseDown"
    />
  </svg>
</template>

<script>
export default {
  name: 'ConnectionLine',
  props: {
    source: {
      type: Object,
      required: true
    },
    target: {
      type: Object,
      required: true
    },
    id: {
      type: [String, Number],
      required: true
    }
  },
  computed: {
    midPoint() {
      return {
        x: (this.source.x + this.target.x) / 2,
        y: (this.source.y + this.target.y) / 2
      }
    },
    lineStyle() {
      return {
        position: 'absolute',
        top: 0,
        left: 0,
        width: '100%',
        height: '100%',
        pointerEvents: 'none',
        zIndex: 10
      }
    }
  },
  methods: {
    onHandleMouseDown(event) {
      event.stopPropagation()
      this.$emit('handle-mousedown', {
        id: this.id,
        event: event
      })
    }
  }
}
</script>

<style scoped>
.connection-line {
  overflow: visible;
}

.connection-path {
  stroke: #1890ff;
  stroke-width: 2;
  stroke-linecap: round;
}

.connection-handle {
  fill: #1890ff;
  cursor: move;
  pointer-events: all;
}

.connection-handle:hover {
  fill: #40a9ff;
}
</style>