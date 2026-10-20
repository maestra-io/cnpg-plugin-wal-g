package resourcecachingclient

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

type Client struct {
	client.Client
	scheme         *runtime.Scheme
	config         *rest.Config
	mapper         meta.RESTMapper
	watchedGVKs    map[string]bool           // read only once init
	watchedObjects map[string]*watchedObject // mutex'ed
	wmut           sync.RWMutex              // for watchedObjects map
	// cmut (capacity 1) serializes cache-miss handling so concurrent misses on one key start ONE
	// watch; a channel, not a Mutex, so a waiting caller still honors its ctx.
	// ponytail: one lock for all keys; a sidecar caches a handful of objects, so per-key locks buy nothing.
	cmut chan struct{}
	// startWatch opens the per-object watch; a field so tests can count watches.
	startWatch func(ctx context.Context, mapping *meta.RESTMapping, key client.ObjectKey) (watch.Interface, error)
	ctx        context.Context // context for long-running operations (ex. Watch requests)
}

// CreateClient creates a new client layer that sits on top of the provided `underlying` client.
// This client implements Get for the provided GVKs, using a specific cache.
//
// Other kinds of requests (ie. non-get and non-managed GVKs) are forwarded to the `underlying`
// client.
//
// Furthermore, cached objects are also available as `source.Source`.
func CreateClient(ctx context.Context, mgr manager.Manager, cachedObjectTypes []client.Object) (*Client, error) {
	watchedGVKs := make(map[string]bool)

	for _, obj := range cachedObjectTypes {
		gvk, err := mgr.GetClient().GroupVersionKindFor(obj)
		if err != nil {
			return nil, err
		}
		watchedGVKs[gvk.String()] = true
	}

	c := &Client{
		Client:         mgr.GetClient(),
		watchedGVKs:    watchedGVKs,
		scheme:         mgr.GetScheme(),
		config:         mgr.GetConfig(),
		mapper:         mgr.GetRESTMapper(),
		watchedObjects: make(map[string]*watchedObject),
		ctx:            ctx,
		cmut:           make(chan struct{}, 1),
	}
	c.startWatch = c.typedWatch
	return c, nil
}

type watchedObject struct {
	cached   client.Object
	handlers []handlerOnQueue
	// watcher is the single watch that feeds this entry; nil means no live watch, so Get is a miss.
	watcher watch.Interface
}

type handlerOnQueue struct {
	handler handler.EventHandler
	queue   workqueue.TypedRateLimitingInterface[reconcile.Request]
}

func (c *Client) Get(ctx context.Context, key client.ObjectKey, out client.Object, opts ...client.GetOption) error {
	gvk, err := c.GroupVersionKindFor(out)
	if err != nil {
		return err
	}
	strGVK := gvk.String()
	if _, managed := c.watchedGVKs[strGVK]; managed {
		// Kind is managed by this cache layer => check for watch
		_, err := c.getAndCreateWatchIfNeeded(ctx, out, gvk, key)
		if err != nil {
			return err
		}
		return nil
	}

	return c.Client.Get(ctx, key, out, opts...)
}

func (c *Client) getAndCreateWatchIfNeeded(
	ctx context.Context,
	obj client.Object,
	gvk schema.GroupVersionKind,
	key client.ObjectKey,
) (string, error) {
	strGVK := gvk.String()
	objKey := strGVK + "|" + key.String()

	if hit, err := c.getFromCache(objKey, gvk, key, obj); hit {
		return objKey, err
	}

	select {
	case c.cmut <- struct{}{}:
	case <-ctx.Done():
		return objKey, ctx.Err()
	}
	defer func() { <-c.cmut }()
	// Another goroutine may have started the watch while we waited for the lock.
	if hit, err := c.getFromCache(objKey, gvk, key, obj); hit {
		return objKey, err
	}

	// Live query
	rlog := log.FromContext(ctx).WithName("narrowcache").WithValues("objKey", objKey)
	rlog.Info("Cache miss, doing live query")

	// Get mapping
	mapping, err := c.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return objKey, err
	}

	err = c.typedGet(ctx, mapping, key, obj)
	if err != nil {
		return objKey, err
	}

	// Create watch for later calls
	w, err := c.startWatch(c.ctx, mapping, key)
	if err != nil {
		return objKey, err
	}

	// Store fetched object together with the watch that owns the entry
	c.wmut.Lock()
	ca := c.watchedObjects[objKey]
	if ca == nil {
		ca = &watchedObject{}
		c.watchedObjects[objKey] = ca
	}
	old := ca.watcher
	ca.cached = obj.DeepCopyObject().(client.Object)
	ca.watcher = w
	c.wmut.Unlock()
	if old != nil {
		old.Stop()
	}

	// Start updating goroutine
	go c.updateCache(c.ctx, objKey, w)
	return objKey, nil
}

// getFromCache copies the cached object into obj. hit is false when the key has no live watch.
func (c *Client) getFromCache(objKey string, gvk schema.GroupVersionKind, key client.ObjectKey, obj client.Object) (bool, error) {
	c.wmut.RLock()
	defer c.wmut.RUnlock()
	ca := c.watchedObjects[objKey]
	if ca == nil || ca.watcher == nil {
		return false, nil
	}
	if ca.cached == nil {
		return true, errors.NewNotFound(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, key.Name)
	}
	return true, copyInto(ca.cached, obj)
}

func (c *Client) restClientForMapping(mapping *meta.RESTMapping) (rest.Interface, error) {
	httpClient, err := rest.HTTPClientFor(c.config)
	if err != nil {
		return nil, fmt.Errorf("while creating HTTPClientFor typed watch: %w", err)
	}

	restClient, err := apiutil.RESTClientForGVK(mapping.GroupVersionKind, false, false, c.config, serializer.NewCodecFactory(c.scheme), httpClient)

	if err != nil {
		return nil, fmt.Errorf("while creating RESTClientForGVK %s: %w", mapping.GroupVersionKind.String(), err)
	}

	return restClient, nil
}

func (c *Client) typedGet(ctx context.Context, mapping *meta.RESTMapping, key client.ObjectKey, obj client.Object) error {
	restClient, err := c.restClientForMapping(mapping)
	if err != nil {
		return err
	}

	return restClient.Get().
		Namespace(key.Namespace).
		Resource(mapping.Resource.Resource).
		Name(key.Name).Do(ctx).Into(obj)
}

func (c *Client) typedWatch(ctx context.Context, mapping *meta.RESTMapping, key client.ObjectKey) (watch.Interface, error) {
	logger := log.FromContext(ctx).WithName("narrowcache").V(1) // Debug logs only for this method
	restClient, err := c.restClientForMapping(mapping)
	if err != nil {
		return nil, err
	}

	// ListOption with FieldSelector to authorize watch with resourceName scoped permissions
	opts := metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector(metav1.ObjectNameField, key.Name).String(),
		Watch:         true,
	}

	r := restClient.Get().
		Namespace(key.Namespace).
		Resource(mapping.Resource.Resource).
		VersionedParams(&opts, runtime.NewParameterCodec(c.scheme))

	logger.Info("performing watch request", "url", r.URL().String())
	return r.Watch(ctx)
}

// "Terrible hack" cc directxman12 / sigs.k8s.io/controller-runtime/pkg/cache/internal/cache_reader.go
func copyInto(obj runtime.Object, out client.Object) error {
	// Copy the value of the item in the cache to the returned value
	// TODO(directxman12): this is a terrible hack, pls fix (we should have deepcopyinto)
	outVal := reflect.ValueOf(out)
	objVal := reflect.ValueOf(obj)
	if !objVal.Type().AssignableTo(outVal.Type()) {
		return fmt.Errorf("cache had type %s, but %s was asked for", objVal.Type(), outVal.Type())
	}
	reflect.Indirect(outVal).Set(reflect.Indirect(objVal))
	// if !c.disableDeepCopy {
	// 	out.GetObjectKind().SetGroupVersionKind(c.groupVersionKind)
	// }
	return nil
}

func (c *Client) updateCache(ctx context.Context, key string, watcher watch.Interface) {
	logger := log.FromContext(ctx).WithName("narrowcache").V(1) // Debug logs only for this method
	for watchEvent := range watcher.ResultChan() {
		logger.WithValues("key", key, "event type", watchEvent.Type, "event data", watchEvent.Object).Info("Event received")
		var prior client.Object
		switch watchEvent.Type {
		case watch.Modified:
			prior = c.snapshotCached(key)
			if err := c.setToCache(key, watcher, watchEvent.Object); err != nil {
				logger.WithValues("key", key).Error(err, "Error while updating cache")
			}
		case watch.Added:
			if err := c.setToCache(key, watcher, watchEvent.Object); err != nil {
				logger.WithValues("key", key).Error(err, "Error while updating cache")
			}
		case watch.Deleted:
			prior = c.snapshotCached(key)
			c.removeFromCache(key, watcher)
		}
		c.callHandlers(ctx, key, watchEvent, prior)
	}
	logger.WithValues("key", key).Info("Watch terminated. Clearing cache entry.")
	// Only the owning watch may drop the entry: a replaced or stopped watch must not
	// clear the entry a newer watch now feeds (that is what used to leak watches).
	c.wmut.Lock()
	if ca := c.watchedObjects[key]; ca != nil && ca.watcher == watcher {
		delete(c.watchedObjects, key)
	}
	c.wmut.Unlock()
}

// snapshotCached returns a deep copy of the cached object before a watch update, or nil if none.
func (c *Client) snapshotCached(key string) client.Object {
	c.wmut.RLock()
	defer c.wmut.RUnlock()
	ca := c.watchedObjects[key]
	if ca == nil || ca.cached == nil {
		return nil
	}
	return ca.cached.DeepCopyObject().(client.Object)
}

// setToCache stores an event's object, but only while watcher still owns the entry.
func (c *Client) setToCache(key string, watcher watch.Interface, obj runtime.Object) error {
	cObj, ok := obj.(client.Object)
	if !ok {
		return fmt.Errorf("could not convert runtime.Object to client.Object")
	}
	c.wmut.Lock()
	defer c.wmut.Unlock()
	if ca := c.watchedObjects[key]; ca != nil && ca.watcher == watcher {
		ca.cached = cObj
	}
	return nil
}

func (c *Client) removeFromCache(key string, watcher watch.Interface) {
	c.wmut.Lock()
	defer c.wmut.Unlock()
	if ca := c.watchedObjects[key]; ca != nil && ca.watcher == watcher {
		ca.cached = nil
	}
}

func (c *Client) addHandler(key string, hoq handlerOnQueue) {
	c.wmut.Lock()
	defer c.wmut.Unlock()
	ca := c.watchedObjects[key]
	if ca == nil {
		c.watchedObjects[key] = &watchedObject{handlers: []handlerOnQueue{hoq}}
		return
	}
	ca.handlers = append(ca.handlers, hoq)
}

func (c *Client) callHandlers(ctx context.Context, key string, ev watch.Event, prior client.Object) {
	var fn func(hoq handlerOnQueue)
	switch ev.Type {
	case watch.Added:
		fn = func(hoq handlerOnQueue) {
			createEvent := event.CreateEvent{Object: ev.Object.(client.Object)}
			hoq.handler.Create(ctx, createEvent, hoq.queue)
		}
	case watch.Modified:
		newObj := ev.Object.(client.Object)
		fn = func(hoq handlerOnQueue) {
			if prior == nil {
				// No prior cache entry (e.g. missed Added); treat like create for handlers.
				createEvent := event.CreateEvent{Object: newObj}
				hoq.handler.Create(ctx, createEvent, hoq.queue)
				return
			}
			modEvent := event.UpdateEvent{ObjectOld: prior, ObjectNew: newObj}
			hoq.handler.Update(ctx, modEvent, hoq.queue)
		}
	case watch.Deleted:
		fn = func(hoq handlerOnQueue) {
			delEvent := event.DeleteEvent{Object: ev.Object.(client.Object)}
			hoq.handler.Delete(ctx, delEvent, hoq.queue)
		}
	case watch.Bookmark:
	case watch.Error:
		// Not managed
	}
	if fn == nil {
		return
	}
	c.wmut.RLock()
	defer c.wmut.RUnlock()
	if ca := c.watchedObjects[key]; ca != nil {
		for _, hoq := range ca.handlers {
			h := hoq
			go fn(h)
		}
	}
}

func (c *Client) GetSource(ctx context.Context, obj client.Object, h handler.EventHandler) (source.Source, error) {
	// Prepare a Source and make sure it is associated with a watch
	rlog := log.FromContext(ctx).WithName("narrowcache")
	rlog.WithValues("name", obj.GetName(), "namespace", obj.GetNamespace()).Info("Getting Source:")
	gvk, err := c.GroupVersionKindFor(obj)
	if err != nil {
		return nil, err
	}
	strGVK := gvk.String()
	_, managed := c.watchedGVKs[strGVK]
	if !managed {
		return nil, fmt.Errorf("called 'GetSource' on unmanaged GVK: %s", strGVK)
	}

	key, err := c.getAndCreateWatchIfNeeded(ctx, obj, gvk, client.ObjectKeyFromObject(obj))
	if err != nil {
		return nil, err
	}

	return &NarrowSource{
		handler: h,
		onStart: func(_ context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			c.addHandler(key, handlerOnQueue{handler: h, queue: q})
		},
	}, nil
}

func (c *Client) clearEntry(ctx context.Context, obj client.Object) {
	key := types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}
	gvk, _ := c.GroupVersionKindFor(obj)
	strGVK := gvk.String()
	if _, managed := c.watchedGVKs[strGVK]; managed {
		log.FromContext(ctx).
			WithName("narrowcache").
			WithValues("name", obj.GetName(), "namespace", obj.GetNamespace()).
			Info("Invalidating cache entry")
		strGVK := gvk.String()
		objKey := strGVK + "|" + key.String()
		c.clearEntryByKey(objKey)
	}
}

// clearEntryByKey drops the entry and stops its watch; the next Get starts a fresh one.
func (c *Client) clearEntryByKey(key string) {
	c.wmut.Lock()
	ca := c.watchedObjects[key]
	delete(c.watchedObjects, key)
	c.wmut.Unlock()
	if ca != nil && ca.watcher != nil {
		ca.watcher.Stop()
	}
}

func (c *Client) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if err := c.Client.Create(ctx, obj, opts...); err != nil {
		// might be due to an outdated cache, clear the corresponding entry
		c.clearEntry(ctx, obj)
		return err
	}
	return nil
}

func (c *Client) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if err := c.Client.Delete(ctx, obj, opts...); err != nil {
		// might be due to an outdated cache, clear the corresponding entry
		c.clearEntry(ctx, obj)
		return err
	}
	return nil
}

func (c *Client) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if err := c.Client.Update(ctx, obj, opts...); err != nil {
		// might be due to an outdated cache, clear the corresponding entry
		c.clearEntry(ctx, obj)
		return err
	}
	return nil
}
